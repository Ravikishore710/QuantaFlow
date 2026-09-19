import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


@dataclass
class ManifestRecord:
    dataset: str
    period: str
    source_url: str
    source_format: str
    downloaded_at: str
    sha256: str
    file_size_bytes: int
    row_count: int
    schema_hash: str
    pipeline_version: str
    status: str
    object_path: str = "not_uploaded"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ManifestStore:
    def __init__(self, manifest_dir: Path | None = None, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.manifest_dir = manifest_dir or self.settings.manifest_dir
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self._db_conn = None

    def _path_for(self, dataset: str, period: str) -> Path:
        dataset_dir = self.manifest_dir / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return dataset_dir / f"{period}.json"

    def _get_db_connection(self) -> Any:
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=self.settings.postgres_host,
                port=self.settings.postgres_port,
                dbname=self.settings.postgres_db,
                user=self.settings.postgres_user,
                password=self.settings.postgres_password,
                connect_timeout=2,
            )
            return conn
        except Exception:
            return None

    def exists(self, dataset: str, period: str, sha256: str) -> bool:
        # Check Postgres first if available
        conn = self._get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1 FROM monitoring.file_manifest
                        WHERE source = %s AND period = %s AND sha256 = %s
                          AND processing_status = 'validated'
                        LIMIT 1;
                        """,
                        (dataset, period, sha256),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        conn.close()
                        return True
            except Exception as exc:
                logger.debug("postgres_manifest_check_fallback", error=str(exc))
            finally:
                conn.close()

        # Fallback to local JSON file
        path = self._path_for(dataset, period)
        if not path.exists():
            return False
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            return existing.get("sha256") == sha256 and existing.get("status") == "validated"
        except Exception:
            return False

    def save(self, record: ManifestRecord) -> Path:
        # Save to local JSON
        path = self._path_for(record.dataset, record.period)
        path.write_text(json.dumps(record.to_dict(), indent=2), encoding="utf-8")

        # Save to Postgres if available
        conn = self._get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO monitoring.file_manifest (
                            source, period, source_url, object_path, sha256,
                            size_bytes, row_count, schema_hash, first_seen_at,
                            last_processed_at, processing_status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, period, sha256)
                        DO UPDATE SET
                            object_path = EXCLUDED.object_path,
                            row_count = EXCLUDED.row_count,
                            last_processed_at = EXCLUDED.last_processed_at,
                            processing_status = EXCLUDED.processing_status;
                        """,
                        (
                            record.dataset,
                            record.period,
                            record.source_url,
                            record.object_path,
                            record.sha256,
                            record.file_size_bytes,
                            record.row_count,
                            record.schema_hash,
                            record.downloaded_at,
                            record.downloaded_at,
                            record.status,
                        ),
                    )
                conn.commit()
                logger.info("postgres_manifest_saved", dataset=record.dataset, period=record.period)
            except Exception as exc:
                logger.debug("postgres_manifest_save_fallback", error=str(exc))
            finally:
                conn.close()

        return path

    def load(self, dataset: str, period: str) -> ManifestRecord | None:
        path = self._path_for(dataset, period)
        if not path.exists():
            return None
        return ManifestRecord(**json.loads(path.read_text(encoding="utf-8")))


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
