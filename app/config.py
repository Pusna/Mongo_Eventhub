"""Єдине місце для налаштувань підключення.

Значення читаються зі змінних оточення або з файлу .env у корені проєкту
(шаблон: .env.example). Паролі в коді не зберігаються.
"""
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Налаштування застосунку з .env."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mongo_hosts: str = "mongo-primary:27017,mongo-secondary1:27018,mongo-secondary2:27019"
    mongo_replica_set: str = "rs0"
    mongo_db: str = "eventhub"
    mongo_test_db: str = "eventhub_test"

    mongo_app_user: str = "app_user"
    mongo_app_password: str = ""
    mongo_auth_source: str = "eventhub"

    mongo_admin_user: str = "admin"
    mongo_admin_password: str = ""

    mongos_host: str = "localhost:27025"

    atlas_uri: Optional[str] = None
    atlas_public_key: Optional[str] = None
    atlas_private_key: Optional[str] = None
    atlas_group_id: Optional[str] = None
    atlas_app_id: Optional[str] = None

    api_change_streams: bool = False
    cors_origins: List[str] = ["*"]

    backup_dir: str = str(PROJECT_ROOT / "backups")
    backup_container: str = "mongo-primary"
    alert_log_file: str = str(PROJECT_ROOT / "mongodb_monitor.log")
    smtp_server: Optional[str] = None
    alert_email_from: Optional[str] = None
    alert_email_to: Optional[str] = None
    slack_webhook: Optional[str] = None

    @property
    def mongo_uri(self) -> str:
        """Рядок підключення застосунку до replica set rs0."""
        return (
            f"mongodb://{quote_plus(self.mongo_app_user)}:{quote_plus(self.mongo_app_password)}"
            f"@{self.mongo_hosts}/{self.mongo_db}"
            f"?replicaSet={self.mongo_replica_set}&authSource={self.mongo_auth_source}"
        )

    @property
    def mongo_admin_uri(self) -> str:
        """Рядок підключення адміністратора до replica set rs0."""
        return (
            f"mongodb://{quote_plus(self.mongo_admin_user)}:{quote_plus(self.mongo_admin_password)}"
            f"@{self.mongo_hosts}/admin?replicaSet={self.mongo_replica_set}&authSource=admin"
        )

    @property
    def mongos_uri(self) -> str:
        """Рядок підключення адміністратора до mongos-router."""
        return (
            f"mongodb://{quote_plus(self.mongo_admin_user)}:{quote_plus(self.mongo_admin_password)}"
            f"@{self.mongos_host}/admin?authSource=admin"
        )


settings = Settings()
