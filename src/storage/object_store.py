import shutil
from pathlib import Path

from src.common.logging import get_logger
from src.config.settings import Settings

logger = get_logger(__name__)


class ObjectStore:
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.minio_bucket
        self.settings = settings
        self.client = None
        self.local_raw_dir = Path("data/raw")
        self.local_raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            import boto3
            from botocore.client import Config

            self.client = boto3.client(
                "s3",
                endpoint_url=settings.minio_endpoint,
                aws_access_key_id=settings.minio_access_key,
                aws_secret_access_key=settings.minio_secret_key,
                config=Config(signature_version="s3v4", connect_timeout=2, read_timeout=2),
                region_name="us-east-1",
            )
        except Exception as exc:
            logger.debug("boto3_init_fallback", error=str(exc))
            self.client = None

    def ensure_bucket(self) -> None:
        if not self.client:
            return
        try:
            existing_buckets = [b["Name"] for b in self.client.list_buckets().get("Buckets", [])]
            if self.bucket not in existing_buckets:
                self.client.create_bucket(Bucket=self.bucket)
                logger.info("bucket_created", bucket=self.bucket)
        except Exception as exc:
            logger.debug("ensure_bucket_fallback", error=str(exc))
            self.client = None

    def upload_file(self, local_path: Path, object_key: str) -> str:
        if self.client:
            try:
                self.client.upload_file(str(local_path), self.bucket, object_key)
                logger.info("object_uploaded", bucket=self.bucket, key=object_key)
                return f"s3://{self.bucket}/{object_key}"
            except Exception as exc:
                logger.debug("s3_upload_failed_fallback_to_local", error=str(exc))

        # Local filesystem raw store fallback
        dest = self.local_raw_dir / object_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)
        logger.info("local_raw_stored", path=str(dest))
        return f"file://{dest.resolve()}"

    def object_exists(self, object_key: str) -> bool:
        if self.client:
            try:
                self.client.head_object(Bucket=self.bucket, Key=object_key)
                return True
            except Exception:
                pass

        dest = self.local_raw_dir / object_key
        return dest.exists()


def raw_object_key(dataset: str, period: str, file_name: str) -> str:
    return f"raw/{dataset}/period={period}/{file_name}"
