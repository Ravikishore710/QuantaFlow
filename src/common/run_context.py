import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from src.common.logging import set_correlation_context


def new_run_id() -> str:
    return uuid.uuid4().hex


@dataclass
class RunContext:
    run_id: str = field(default_factory=new_run_id)
    correlation_id: str = field(default_factory=new_run_id)
    pipeline_name: str = "quanta_flow_monthly_pipeline"
    pipeline_version: str = "0.1.0"
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        set_correlation_context(correlation_id=self.correlation_id, run_id=self.run_id)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "correlation_id": self.correlation_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.started_at.isoformat(),
        }
