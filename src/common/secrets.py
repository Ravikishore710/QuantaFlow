import os
from pathlib import Path

from pydantic import SecretStr

from src.common.logging import get_logger, redact_sensitive_processor

logger = get_logger(__name__)

# Re-export for convenience
__all__ = ["SecretManager", "redact_sensitive_processor"]


class SecretManager:
    """Multi-backend hierarchical secret manager supporting:
    1. Docker/Kubernetes mounted secrets (/run/secrets/<name> or custom SECRETS_DIR).
    2. AWS Secrets Manager (via boto3 when AWS_SECRETS_PREFIX configured).
    3. Environment variables with pydantic.SecretStr wrapping.
    """

    def __init__(
        self,
        secrets_dir: Path | str | None = None,
        aws_secrets_prefix: str | None = None,
        aws_region: str = "us-east-1",
    ) -> None:
        self.secrets_dir = Path(secrets_dir or os.environ.get("SECRETS_DIR", "/run/secrets"))
        self.aws_secrets_prefix = aws_secrets_prefix or os.environ.get("AWS_SECRETS_PREFIX")
        self.aws_region = aws_region
        self._cache: dict[str, SecretStr] = {}

    def get_secret(self, name: str, default: str | None = None) -> SecretStr | None:
        """Resolve a secret from file mount, AWS Secrets Manager, or environment."""
        if name in self._cache:
            return self._cache[name]

        # 1. Check mounted file secret
        file_path = self.secrets_dir / name
        if file_path.exists() and file_path.is_file():
            try:
                secret_text = file_path.read_text(encoding="utf-8").strip()
                sec = SecretStr(secret_text)
                self._cache[name] = sec
                logger.debug("secret_loaded_from_file", secret=name, source=str(file_path))
                return sec
            except Exception as exc:
                logger.warning("secret_file_read_error", secret=name, error=str(exc))

        # 2. Check AWS Secrets Manager
        if self.aws_secrets_prefix:
            try:
                import boto3

                client = boto3.client("secretsmanager", region_name=self.aws_region)
                secret_id = f"{self.aws_secrets_prefix.rstrip('/')}/{name}"
                resp = client.get_secret_value(SecretId=secret_id)
                if "SecretString" in resp:
                    sec = SecretStr(resp["SecretString"])
                    self._cache[name] = sec
                    logger.debug("secret_loaded_from_aws", secret=name, secret_id=secret_id)
                    return sec
            except Exception as exc:
                logger.debug("aws_secrets_manager_lookup_skipped", secret=name, reason=str(exc))

        # 3. Check environment variable fallback
        env_val = os.environ.get(name.upper()) or os.environ.get(name)
        if env_val is not None:
            sec = SecretStr(env_val)
            self._cache[name] = sec
            return sec

        if default is not None:
            sec = SecretStr(default)
            self._cache[name] = sec
            return sec

        return None

    def get_secret_value(self, name: str, default: str = "") -> str:
        """Helper returning the unmasked string value of a resolved secret."""
        sec = self.get_secret(name, default=default)
        return sec.get_secret_value() if sec else default
