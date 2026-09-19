import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime


def new_run_id() -> str:
    return uuid.uuid4().hex


@dataclass
class RunContext:
    run_id: str = field(default_factory=new_run_id)
    pipeline_name: str = "quanta_flow_monthly_pipeline"
    pipeline_version: str = "0.1.0"
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.started_at.isoformat(),
        }
