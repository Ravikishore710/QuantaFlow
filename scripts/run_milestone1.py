import argparse
import sys

from src.common.hashing import schema_hash, sha256_file
from src.common.logging import configure_logging, get_logger
from src.config.settings import get_settings
from src.ingestion.manifest import ManifestRecord, ManifestStore, utc_now_iso
from src.sources.tlc_taxi import TLCTaxiSource
from src.storage.object_store import ObjectStore, raw_object_key

logger = get_logger(__name__)


def run(period: str, skip_upload: bool) -> int:
    configure_logging()
    settings = get_settings()
    source = TLCTaxiSource()
    manifest_store = ManifestStore(settings.manifest_dir)

    discovered = source.discover(period)
    logger.info("source_discovered", dataset=discovered.dataset, url=discovered.source_url)

    downloaded = source.download(discovered, settings.sample_dir)
    checksum = sha256_file(downloaded.local_path)
    logger.info("checksum_computed", sha256=checksum)

    if manifest_store.exists(discovered.dataset, period, checksum):
        logger.info("skip_already_processed", dataset=discovered.dataset, period=period)
        return 0

    validation = source.validate(downloaded)
    if not validation.passed:
        logger.error("validation_failed", errors=validation.errors)
        manifest_store.save(
            ManifestRecord(
                dataset=discovered.dataset,
                period=period,
                source_url=discovered.source_url,
                source_format=discovered.source_format,
                downloaded_at=utc_now_iso(),
                sha256=checksum,
                file_size_bytes=downloaded.file_size_bytes,
                row_count=validation.row_count,
                schema_hash=schema_hash(validation.column_names, validation.column_types),
                pipeline_version=settings.pipeline_version,
                status="validation_failed",
            )
        )
        return 1

    object_path = "not_uploaded"
    if not skip_upload:
        store = ObjectStore(settings)
        store.ensure_bucket()
        key = raw_object_key(discovered.dataset, period, downloaded.local_path.name)
        object_path = store.upload_file(downloaded.local_path, key)

    manifest_store.save(
        ManifestRecord(
            dataset=discovered.dataset,
            period=period,
            source_url=discovered.source_url,
            source_format=discovered.source_format,
            downloaded_at=utc_now_iso(),
            sha256=checksum,
            file_size_bytes=downloaded.file_size_bytes,
            row_count=validation.row_count,
            schema_hash=schema_hash(validation.column_names, validation.column_types),
            pipeline_version=settings.pipeline_version,
            status="validated",
        )
    )

    logger.info(
        "milestone1_complete",
        dataset=discovered.dataset,
        period=period,
        row_count=validation.row_count,
        object_path=object_path,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the QuantaFlow milestone 1 ingestion")
    parser.add_argument("--period", default="2024-01", help="Period in YYYY-MM format")
    parser.add_argument(
        "--skip-upload", action="store_true", help="Skip MinIO upload, download and validate only"
    )
    args = parser.parse_args()
    sys.exit(run(args.period, args.skip_upload))


if __name__ == "__main__":
    main()
