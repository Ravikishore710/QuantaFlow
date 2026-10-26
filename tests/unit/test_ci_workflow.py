"""Unit tests verifying GitHub Actions CI/CD pipeline configuration."""

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).parent.parent.parent
CI_WORKFLOW_PATH = PROJECT_ROOT / ".github" / "workflows" / "ci.yml"


def test_ci_workflow_structure() -> None:
    """Verify that CI workflow file exists, parses as valid YAML, and contains all 4 stages."""
    assert CI_WORKFLOW_PATH.is_file(), "ci.yml workflow file missing"

    content = CI_WORKFLOW_PATH.read_text(encoding="utf-8")
    data = yaml.safe_load(content)

    assert "jobs" in data, "No jobs declared in ci.yml"
    jobs = data["jobs"]

    expected_jobs = [
        "quality-and-tests",
        "fresh-clone-check",
        "iac-validation",
        "deploy",
    ]
    for job_name in expected_jobs:
        assert job_name in jobs, f"Missing required CI job: {job_name}"


def test_ci_workflow_quality_gates() -> None:
    """Verify lock check, coverage floor, fresh clone, and terraform validation in CI steps."""
    content = CI_WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "uv lock --check" in content
    assert "cov-fail-under=90" in content
    assert "scripts/verify_fresh_clone.sh" in content
    assert "terraform validate" in content
    assert "docker build" in content

    data = yaml.safe_load(content)
    deploy_job = data["jobs"]["deploy"]
    assert "needs" in deploy_job
    assert set(deploy_job["needs"]) == {
        "quality-and-tests",
        "fresh-clone-check",
        "iac-validation",
    }
