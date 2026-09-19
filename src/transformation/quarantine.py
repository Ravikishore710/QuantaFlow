import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


@dataclass
class QuarantineRecord:
    dataset: str
    period: str
    stage: str
    reason: str
    rejected_at: str
    raw_record: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class QuarantineStore:
    def __init__(
        self, quarantine_dir: Path | None = None, settings: Settings | None = None
    ) -> None:
        self.settings = settings or get_settings()
        self.quarantine_dir = quarantine_dir or self.settings.quarantine_dir
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def route_records(
        self,
        dataset: str,
        period: str,
        stage: str,
        rejected_rows: list[dict[str, Any]],
        reason: str,
    ) -> int:
        if not rejected_rows:
            return 0

        target_dir = self.quarantine_dir / dataset / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)

        timestamp_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        output_file = target_dir / f"quarantine_{stage}_{timestamp_str}.jsonl"

        now_iso = datetime.now(UTC).isoformat()
        records = [
            QuarantineRecord(
                dataset=dataset,
                period=period,
                stage=stage,
                reason=reason,
                rejected_at=now_iso,
                raw_record=row,
            )
            for row in rejected_rows
        ]

        with output_file.open("a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record.to_dict(), default=str) + "\n")

        logger.warn(
            "records_quarantined",
            dataset=dataset,
            period=period,
            stage=stage,
            count=len(records),
            path=str(output_file),
        )
        return len(records)

    def get_rejected_count(self, dataset: str, period: str) -> int:
        target_dir = self.quarantine_dir / dataset / f"period={period}"
        if not target_dir.exists():
            return 0

        total = 0
        for f in target_dir.glob("*.jsonl"):
            with f.open("r", encoding="utf-8") as handle:
                total += sum(1 for _ in handle)
        return total
