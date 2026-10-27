"""Unit tests verifying pipeline CLI dry-run mode, JSON formatting, and argument parsing."""

import json
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from scripts.run_pipeline import main, run_pipeline


def test_pipeline_dry_run_execution() -> None:
    """Verify dry_run executes non-mutating validation and returns 0 exit code."""
    code = run_pipeline(period="2024-01", skip_upload=True, dry_run=True, output_json=False)
    assert code == 0


def test_pipeline_dry_run_json_output(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify --output-json writes structured JSON summary to stdout."""
    code = run_pipeline(period="2024-01", skip_upload=True, dry_run=True, output_json=True)
    assert code == 0

    captured = capsys.readouterr()
    assert captured.out
    # structlog outputs json log line first, followed by final structured stdout payload
    last_line = captured.out.strip().splitlines()[-1]
    data = json.loads(last_line)
    assert data["status"] == "dry_run_success"
    assert data["period"] == "2024-01"
    assert data["dry_run"] is True


def test_pipeline_cli_main_entrypoint() -> None:
    """Verify CLI main entrypoint parses arguments and invokes run_pipeline."""
    test_args = ["run_pipeline.py", "--period", "2024-02", "--dry-run", "--skip-upload"]
    with patch("sys.argv", test_args):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 0


def test_pipeline_cli_invalid_period_fails() -> None:
    """Verify invalid period string format fails ingress validation."""
    with pytest.raises(ValidationError):
        run_pipeline(period="invalid-date", dry_run=True)
