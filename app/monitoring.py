"""Моніторинг та адміністрування."""
import glob
import json
import logging
import os
import smtplib
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Callable, Dict, List, Optional

from pymongo import DESCENDING, MongoClient

from app.config import settings

logger = logging.getLogger(__name__)


# ---------- Performance Monitoring ----------

class PerformanceMonitor:
    """Метрики сервера, реплікації та повільних запитів (розділ 12.1)."""

    def __init__(self, client: MongoClient, db_name: Optional[str] = None) -> None:
        """client — підключення до кластера; db_name — база застосунку для профайлера."""
        self.client = client
        self.db = client.admin
        self.app_db = client[db_name or settings.mongo_db]

    def get_server_status(self) -> Dict:
        """Отримати статус сервера"""
        status = self.db.command("serverStatus")

        return {
            "connections": {
                "current": status["connections"]["current"],
                "available": status["connections"]["available"],
                "total_created": status["connections"]["totalCreated"]
            },
            "memory": {
                "resident_mb": status["mem"]["resident"],
                "virtual_mb": status["mem"]["virtual"]
            },
            "operations": {
                "inserts": status["opcounters"]["insert"],
                "queries": status["opcounters"]["query"],
                "updates": status["opcounters"]["update"],
                "deletes": status["opcounters"]["delete"],
                "commands": status["opcounters"]["command"]
            },
            "storage": {
                "data_size_mb": status.get("wiredTiger", {}).get("cache", {}).get(
                    "bytes currently in the cache", 0) / 1024 / 1024
            }
        }

    def get_replication_status(self) -> Dict:
        """Статус реплікації"""
        status = self.db.command("replSetGetStatus")

        primary_optime = next((m["optimeDate"] for m in status["members"]
                               if m["stateStr"] == "PRIMARY"), None)
        members = []
        for member in status["members"]:
            lag = None
            if primary_optime and member.get("optimeDate") and member["stateStr"] == "SECONDARY":
                lag = (primary_optime - member["optimeDate"]).total_seconds()
            members.append({
                "name": member["name"],
                "state": member["stateStr"],
                "health": member["health"],
                "optime": member.get("optime", {}).get("ts"),
                "lag_seconds": lag
            })

        return {
            "set_name": status["set"],
            "my_state": status["myState"],
            "members": members,
            "sync_source": status.get("syncSourceHost") or status.get("syncingTo")
        }

    def get_top_command_usage(self) -> List[Dict]:
        """Топ команд за часом виконання"""
        top = self.db.command("top")

        results = []
        for collection, stats in top["totals"].items():
            if not isinstance(stats, dict):
                continue
            results.append({
                "collection": collection,
                "total_time": stats["total"]["time"],
                "read_time": stats["readLock"]["time"],
                "write_time": stats["writeLock"]["time"],
                "queries": stats.get("queries", {}).get("count", 0)
            })

        return sorted(results, key=lambda x: x["total_time"], reverse=True)[:10]

    def get_slow_queries(self, threshold_ms: int = 100) -> List[Dict]:
        """Отримати повільні запити з system.profile"""
        self.app_db.command("profile", 1, slowms=threshold_ms)

        slow_queries = list(
            self.app_db.system.profile.find({
                "millis": {"$gt": threshold_ms}
            }).sort([("ts", DESCENDING)]).limit(50)
        )

        return slow_queries

    def monitor_connections(self, iterations: Optional[int] = None, interval: float = 5,
                            on_sample: Optional[Callable[[Dict], None]] = None) -> List[Dict]:
        """Моніторинг з'єднань в реальному часі (iterations=None — нескінченно, як у PDF)"""
        samples = []
        i = 0
        while iterations is None or i < iterations:
            status = self.get_server_status()
            sample = {"time": datetime.now(timezone.utc), "connections": status["connections"]["current"]}
            logger.info(f"[{sample['time']}] Connections: {sample['connections']}")
            samples.append(sample)
            if on_sample:
                on_sample(sample)
            i += 1
            if iterations is None or i < iterations:
                time.sleep(interval)
        return samples


# ---------- Backup and Recovery ----------

class BackupManager:

    def __init__(self, host: str = "localhost", port: int = 27017,
                 backup_dir: Optional[str] = None,
                 exec_prefix: Optional[List[str]] = None,
                 runner: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> None:
        """exec_prefix — запуск утиліт через docker exec; runner підміна для тестів."""
        self.host = host
        self.port = port
        self.backup_dir = backup_dir or settings.backup_dir
        self.exec_prefix = (exec_prefix if exec_prefix is not None
                            else ["docker", "exec", "-i", settings.backup_container])
        self.runner = runner
        os.makedirs(self.backup_dir, exist_ok=True)

    def _base(self, tool: str) -> List[str]:
        """Початок команди mongodump/mongorestore з хостом і портом."""
        return [*self.exec_prefix, tool, f"--host={self.host}", f"--port={self.port}"]

    def _dump_to_file(self, args: List[str], backup_path: str) -> Optional[str]:
        """Запустити mongodump у stdout і записати архів у файл; помилка -> None."""
        with open(backup_path, "wb") as out:
            result = self.runner([*self._base("mongodump"), *args, "--archive", "--gzip"],
                                 stdout=out, stderr=subprocess.PIPE)
        if result.returncode == 0:
            logger.info(f"Backup successful: {backup_path}")
            return backup_path
        logger.error(f"Backup failed: {result.stderr}")
        return None

    def perform_full_backup(self, db_name: str) -> Optional[str]:
        """Повний бекап бази даних (gzip-архів)"""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.backup_dir, f"{db_name}_{timestamp}.archive.gz")
        return self._dump_to_file([f"--db={db_name}"], backup_path)

    def perform_incremental_backup(self) -> Optional[str]:
        """Бекап усього інстансу з oplog (--oplog працює лише без --db)"""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.backup_dir, f"oplog_{timestamp}.archive.gz")
        return self._dump_to_file(["--oplog"], backup_path)

    def restore_from_backup(self, backup_path: str, ns_from: Optional[str] = None,
                            ns_to: Optional[str] = None, drop: bool = True,
                            oplog_limit: Optional[str] = None) -> bool:

        """Відновлення з бекапу"""
        cmd = [*self._base("mongorestore"), "--archive", "--gzip"]
        if drop:
            cmd.append("--drop")
        if ns_from and ns_to:
            cmd += [f"--nsFrom={ns_from}", f"--nsTo={ns_to}"]
        if oplog_limit:
            cmd += ["--oplogReplay", f"--oplogLimit={oplog_limit}"]
        with open(backup_path, "rb") as src:
            result = self.runner(cmd, stdin=src, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            logger.info("Restore successful!")
            return True
        logger.error(f"Restore failed: {result.stderr}")
        return False

    def list_backups(self) -> List[Dict]:
        """Список архівів у backup_dir, від старих до нових."""
        backups = []
        for path in glob.glob(os.path.join(self.backup_dir, "*.archive.gz")):
            stamp = os.path.basename(path).rsplit("_", 2)[-2:]
            try:
                taken_at = datetime.strptime("_".join(stamp).split(".")[0], "%Y%m%d_%H%M%S")
            except ValueError:
                continue
            backups.append({"path": path, "taken_at": taken_at.replace(tzinfo=timezone.utc)})
        return sorted(backups, key=lambda b: b["taken_at"])

    def point_in_time_recovery(self, target_time: datetime, apply: bool = False,
                               ns_from: Optional[str] = None,
                               ns_to: Optional[str] = None) -> Dict:
        """Відновлення на певний момент часу"""
        if target_time.tzinfo is None:
            target_time = target_time.replace(tzinfo=timezone.utc)
        candidates = [b for b in self.list_backups()
                      if os.path.basename(b["path"]).startswith("oplog_")
                      and b["taken_at"] <= target_time]
        if not candidates:
            return {"possible": False, "reason": "no oplog backup before target_time"}
        backup = candidates[-1]
        plan = {"possible": True, "backup": backup["path"],
                "oplog_limit": f"{int(target_time.timestamp())}:0",
                "target_time": target_time}
        logger.info(f"Point-in-time recovery to {target_time}: {plan}")
        if apply:
            plan["restored"] = self.restore_from_backup(backup["path"], ns_from, ns_to, oplog_limit=plan["oplog_limit"])
        return plan


# ---------- Alerting та Logging ----------

class AlertManager:

    DEFAULT_THRESHOLDS = {"connections": 80, "resident_mb": 1024, "lag_seconds": 60}

    def __init__(self, config: Optional[Dict] = None,
                 http_post: Optional[Callable[[str, Dict], None]] = None,
                 smtp_factory: Callable[[str], smtplib.SMTP] = smtplib.SMTP) -> None:
        """config — пороги та канали; http_post і smtp_factory — підміна для тестів"""
        self.config = config or {}
        self.thresholds = {**self.DEFAULT_THRESHOLDS, **self.config.get("thresholds", {})}
        self._http_post = http_post or self._urllib_post
        self._smtp_factory = smtp_factory
        self.setup_logging()

    def setup_logging(self) -> None:
        """Налаштування логування"""
        self.logger = logging.getLogger("eventhub.alerts")
        self.logger.setLevel(logging.INFO)
        log_file = os.path.abspath(self.config.get("log_file", settings.alert_log_file))
        if not any(getattr(h, "baseFilename", None) == log_file for h in self.logger.handlers):
            handler = logging.FileHandler(log_file, encoding="utf-8")
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)

    def check_thresholds(self, metrics: Dict) -> List[Dict]:
        """Перевірка метрик на перевищення порогів"""
        alerts = []

        limit = self.thresholds["connections"]
        if metrics["connections"]["current"] > limit:
            alerts.append({
                "level": "WARNING",
                "message": f"High connections: {metrics['connections']['current']}",
                "threshold": limit
            })

        limit = self.thresholds["resident_mb"]
        if metrics["memory"]["resident_mb"] > limit:
            alerts.append({
                "level": "CRITICAL",
                "message": f"High memory usage: {metrics['memory']['resident_mb']} MB",
                "threshold": limit
            })

        limit = self.thresholds["lag_seconds"]
        if metrics.get("replication"):
            for member in metrics["replication"]["members"]:
                if (member.get("lag_seconds") or 0) > limit:
                    alerts.append({
                        "level": "WARNING",
                        "message": f"Replication lag on {member['name']}: {member['lag_seconds']}s",
                        "threshold": limit
                    })

        return alerts

    def send_alert(self, alert: Dict) -> None:
        """Відправка сповіщення"""
        if self.config.get("email_enabled"):
            self.send_email_alert(alert)

        if self.config.get("slack_enabled"):
            self.send_slack_alert(alert)

        self.logger.warning(f"Alert: {alert['level']} - {alert['message']}")

    def send_email_alert(self, alert: Dict) -> None:
        """Email сповіщення"""
        msg = MIMEText(f"Alert: {alert['message']}")
        msg['Subject'] = f"MongoDB Alert: {alert['level']}"
        msg['From'] = self.config.get('email_from') or settings.alert_email_from
        msg['To'] = self.config.get('email_to') or settings.alert_email_to

        with self._smtp_factory(self.config.get('smtp_server') or settings.smtp_server) as server:
            server.send_message(msg)

    @staticmethod
    def _urllib_post(url: str, payload: Dict) -> None:
        """POST JSON (вебхук Slack)."""
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=10).close()

    def send_slack_alert(self, alert: Dict) -> None:
        """Slack сповіщення (urllib замість requests — пакета requests немає в залежностях)"""
        webhook_url = self.config.get('slack_webhook') or settings.slack_webhook
        payload = {
            "text": f"*{alert['level']}*: {alert['message']}",
            "username": "MongoDB Monitor"
        }
        self._http_post(webhook_url, payload)
