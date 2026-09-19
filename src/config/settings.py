from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "quantaflow"
    postgres_user: str = "quantaflow"
    postgres_password: str = "changeme"

    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "quantaflow"
    minio_secret_key: str = "changeme123"
    minio_bucket: str = "quanta-flow"

    pipeline_version: str = "0.1.0"
    data_interval_start: str = "2024-01"
    data_interval_end: str = "2024-06"

    manifest_dir: Path = Path("data/manifests")
    sample_dir: Path = Path("data/samples")
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    gold_dir: Path = Path("data/gold")
    quarantine_dir: Path = Path("data/quarantine")
    reports_dir: Path = Path("data/reports")

    weather_station_id: str = "USW00094728"  # Central Park, NYC
    row_count_change_threshold: float = Field(default=0.30, description="Volume anomaly threshold")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
