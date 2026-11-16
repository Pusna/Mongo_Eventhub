"""Моніторинг, бекапи, алерти"""
import os
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from app.config import settings
from app.monitoring import AlertManager, BackupManager, PerformanceMonitor

RESTORE_DB = "eventhub_restore_check"


@pytest.fixture(scope="module")
def monitor(admin_client):
    return PerformanceMonitor(admin_client, settings.mongo_test_db)


def test_server_status(monitor):
    status = monitor.get_server_status()
    assert status["connections"]["current"] > 0
    assert status["memory"]["resident_mb"] > 0
    assert status["storage"]["data_size_mb"] > 0
    assert status["operations"]["commands"] > 0


def test_replication_status(monitor):
    rep = monitor.get_replication_status()
    assert rep["set_name"] == "rs0"
    secondaries = [m for m in rep["members"] if m["state"] == "SECONDARY"]
    assert len(secondaries) == 2
    assert all(m["lag_seconds"] is not None for m in secondaries)


def test_top_command_usage(monitor):
    top = monitor.get_top_command_usage()
    assert 0 < len(top) <= 10
    assert [t["total_time"] for t in top] == sorted((t["total_time"] for t in top), reverse=True)


def test_slow_queries(monitor):
    assert isinstance(monitor.get_slow_queries(threshold_ms=100), list)
    monitor.app_db.command("profile", 0)


def test_monitor_connections(monitor):
    samples = []
    result = monitor.monitor_connections(iterations=2, interval=0.1, on_sample=samples.append)
    assert len(result) == 2 and samples == result


@pytest.fixture(scope="module")
def backups(tmp_path_factory):
    return BackupManager(backup_dir=str(tmp_path_factory.mktemp("backups")))


def docker_available() -> bool:
    try:
        return subprocess.run(["docker", "exec", settings.backup_container, "true"],
                              capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.mark.skipif(not docker_available(), reason="docker / контейнер mongo-primary недоступні")
def test_full_backup_and_restore_to_other_db(backups, ops, admin_client):
    path = backups.perform_full_backup(settings.mongo_test_db)
    assert path and os.path.getsize(path) > 0

    ok = backups.restore_from_backup(path, ns_from=f"{settings.mongo_test_db}.*",
                                     ns_to=f"{RESTORE_DB}.*")
    assert ok
    assert admin_client[RESTORE_DB].users.count_documents({}) >= ops.users.count_documents({}) - 5


@pytest.mark.skipif(not docker_available(), reason="docker / контейнер mongo-primary недоступні")
def test_oplog_backup_and_pitr_plan(backups):
    assert backups.perform_incremental_backup()
    plan = backups.point_in_time_recovery(datetime.now(timezone.utc) + timedelta(seconds=5))
    assert plan["possible"] and plan["backup"].endswith(".archive.gz")
    assert plan["oplog_limit"].endswith(":0")
    assert backups.point_in_time_recovery(datetime(2000, 1, 1))["possible"] is False


def test_backup_failure_and_restore_command(tmp_path):
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 1, b"", b"boom")

    bm = BackupManager(backup_dir=str(tmp_path), exec_prefix=[], runner=fake_run)
    assert bm.perform_full_backup("eventhub") is None
    archive = tmp_path / "x_20260101_000000.archive.gz"
    archive.write_bytes(b"")
    assert bm.restore_from_backup(str(archive), oplog_limit="1:0") is False
    assert "--oplogReplay" in commands[-1] and "--drop" in commands[-1]
    assert commands[0][0] == "mongodump" and "--archive" in commands[0]


def test_pitr_apply_uses_oplog_limit(tmp_path):
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    (tmp_path / "oplog_20260101_000000.archive.gz").write_bytes(b"")
    (tmp_path / "garbage.archive.gz").write_bytes(b"")
    bm = BackupManager(backup_dir=str(tmp_path), exec_prefix=[], runner=fake_run)
    plan = bm.point_in_time_recovery(datetime(2026, 6, 1), apply=True)
    assert plan["restored"] is True
    assert any(arg.startswith("--oplogLimit=") for arg in commands[-1])


def test_alert_thresholds_and_channels(tmp_path):
    posts, mails = [], []

    class FakeSMTP:
        def __init__(self, server):
            self.server = server

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def send_message(self, msg):
            mails.append((self.server, msg["Subject"], msg["To"]))

    log_file = tmp_path / "alerts.log"
    am = AlertManager({"email_enabled": True, "slack_enabled": True, "smtp_server": "smtp.test",
                       "email_from": "a@test", "email_to": "b@test",
                       "slack_webhook": "https://hooks.test/x", "log_file": str(log_file)},
                      http_post=lambda url, payload: posts.append((url, payload)),
                      smtp_factory=FakeSMTP)
    metrics = {"connections": {"current": 100}, "memory": {"resident_mb": 2048},
               "replication": {"members": [{"name": "a", "lag_seconds": None},
                                           {"name": "b", "lag_seconds": 120}]}}
    alerts = am.check_thresholds(metrics)
    assert [a["level"] for a in alerts] == ["WARNING", "CRITICAL", "WARNING"]

    am.send_alert(alerts[1])
    assert mails == [("smtp.test", "MongoDB Alert: CRITICAL", "b@test")]
    assert posts[0][1]["text"].startswith("*CRITICAL*")
    for handler in am.logger.handlers:
        handler.flush()
    assert "High memory usage" in log_file.read_text(encoding="utf-8")


def test_alert_no_alerts_for_healthy_metrics(tmp_path):
    am = AlertManager({"log_file": str(tmp_path / "a.log")})
    healthy = {"connections": {"current": 5}, "memory": {"resident_mb": 100}}
    assert am.check_thresholds(healthy) == []
