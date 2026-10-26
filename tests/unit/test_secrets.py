from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.common.logging import redact_sensitive_processor
from src.common.secrets import SecretManager
from src.config.settings import Settings


def test_secret_manager_file_resolution(tmp_path: Path) -> None:
    secret_file = tmp_path / "postgres_password"
    secret_file.write_text("super_secure_vault_pw\n", encoding="utf-8")

    sm = SecretManager(secrets_dir=tmp_path)
    sec = sm.get_secret("postgres_password")

    assert sec is not None
    assert sec.get_secret_value() == "super_secure_vault_pw"
    # Ensure repr hides value
    assert "super_secure_vault_pw" not in repr(sec)


def test_secret_manager_aws_resolution() -> None:
    mock_boto_client = MagicMock()
    mock_boto_client.get_secret_value.return_value = {
        "SecretString": "aws_managed_secret_123"
    }

    with patch("boto3.client", return_value=mock_boto_client):
        sm = SecretManager(
            secrets_dir=Path("/nonexistent"),
            aws_secrets_prefix="/quantaflow/prod",
        )
        val = sm.get_secret_value("api_token")
        assert val == "aws_managed_secret_123"
        mock_boto_client.get_secret_value.assert_called_once_with(
            SecretId="/quantaflow/prod/api_token"
        )


def test_secret_manager_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_SECRET_KEY", "env_secret_val_456")

    sm = SecretManager(secrets_dir=Path("/nonexistent"))
    val = sm.get_secret_value("app_secret_key")
    assert val == "env_secret_val_456"


def test_secret_manager_default_fallback() -> None:
    sm = SecretManager(secrets_dir=Path("/nonexistent"))
    val = sm.get_secret_value("unknown_secret", default="fallback_val")
    assert val == "fallback_val"


def test_redact_sensitive_processor_masks_credentials() -> None:
    event_dict = {
        "event": "database_connection_attempt",
        "postgres_password": "supersecretpassword",
        "minio_secret_key": "minio_secret_123",
        "auth_token": "bearer_jwt_token_xyz",
        "postgres_dsn": "postgresql://user:pass@localhost:5432/db",
        "sort_keys": ["pickup_datetime"],
        "dataset": "nyc_tlc_yellow",
    }

    redacted = redact_sensitive_processor(logger=None, method_name="info", event_dict=event_dict)

    assert redacted["postgres_password"] == "***REDACTED***"
    assert redacted["minio_secret_key"] == "***REDACTED***"
    assert redacted["auth_token"] == "***REDACTED***"
    assert redacted["postgres_dsn"] == "***REDACTED***"
    # Preserves non-sensitive and structural keys
    assert redacted["sort_keys"] == ["pickup_datetime"]
    assert redacted["dataset"] == "nyc_tlc_yellow"


def test_settings_resolved_postgres_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pw_file = tmp_path / "postgres_password"
    pw_file.write_text("file_mounted_password_999", encoding="utf-8")
    monkeypatch.setenv("SECRETS_DIR", str(tmp_path))

    settings = Settings()
    assert settings.resolved_postgres_password == "file_mounted_password_999"
    assert "file_mounted_password_999" in settings.postgres_dsn
