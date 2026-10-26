"""Unit tests verifying Terraform Infrastructure-as-Code definitions.

Ensures all modular Terraform files adhere to enterprise security and architecture standards.
"""

from pathlib import Path

TERRAFORM_DIR = Path(__file__).parent.parent.parent / "terraform"


def test_terraform_root_files_exist() -> None:
    """Verify all root Terraform files exist and are populated."""
    root_files = [
        "versions.tf",
        "variables.tf",
        "main.tf",
        "outputs.tf",
        "terraform.tfvars.example",
    ]
    for filename in root_files:
        file_path = TERRAFORM_DIR / filename
        assert file_path.is_file(), f"Missing required Terraform root file: {filename}"
        assert file_path.stat().st_size > 0, f"Terraform root file is empty: {filename}"


def test_terraform_modules_structure() -> None:
    """Verify storage, warehouse, and orchestration modules exist with complete file sets."""
    modules = ["storage", "warehouse", "orchestration"]
    expected_files = ["main.tf", "variables.tf", "outputs.tf"]

    for module in modules:
        module_dir = TERRAFORM_DIR / "modules" / module
        assert module_dir.is_dir(), f"Missing Terraform module directory: {module}"
        for filename in expected_files:
            file_path = module_dir / filename
            assert file_path.is_file(), f"Missing file in module '{module}': {filename}"
            assert file_path.stat().st_size > 0, f"Empty file in module '{module}': {filename}"


def test_terraform_storage_security() -> None:
    """Verify S3 storage module enforces encryption and blocks all public access."""
    storage_main = (TERRAFORM_DIR / "modules" / "storage" / "main.tf").read_text(encoding="utf-8")
    assert "aws_s3_bucket_public_access_block" in storage_main
    assert "block_public_acls" in storage_main
    assert "block_public_policy" in storage_main
    assert "ignore_public_acls" in storage_main
    assert "restrict_public_buckets" in storage_main
    assert "aws_s3_bucket_server_side_encryption_configuration" in storage_main
    assert "aws_s3_bucket_versioning" in storage_main


def test_terraform_warehouse_security() -> None:
    """Verify RDS PostgreSQL module enforces storage encryption and manages secrets."""
    warehouse_main = (TERRAFORM_DIR / "modules" / "warehouse" / "main.tf").read_text(
        encoding="utf-8"
    )
    assert "storage_encrypted" in warehouse_main
    assert "true" in warehouse_main
    assert "aws_secretsmanager_secret" in warehouse_main
    assert "random_password" in warehouse_main
    assert "aws_db_instance" in warehouse_main
    assert "engine" in warehouse_main and "postgres" in warehouse_main
    assert "16" in warehouse_main


def test_terraform_orchestration_security() -> None:
    """Verify ECS orchestration defines KMS encrypted log group and health check."""
    orchestration_main = (TERRAFORM_DIR / "modules" / "orchestration" / "main.tf").read_text(
        encoding="utf-8"
    )
    assert "aws_kms_key" in orchestration_main
    assert "enable_key_rotation     = true" in orchestration_main
    assert "aws_cloudwatch_log_group" in orchestration_main
    assert "aws_ecs_cluster" in orchestration_main
    assert "containerInsights" in orchestration_main
    assert "aws_ecs_task_definition" in orchestration_main
    assert "scripts/health_check.py" in orchestration_main


def test_terraform_no_plaintext_passwords() -> None:
    """Verify no hardcoded plaintext passwords exist in any Terraform files."""
    for tf_file in TERRAFORM_DIR.rglob("*.tf"):
        content = tf_file.read_text(encoding="utf-8").lower()
        assert "password = \"password" not in content
        assert "password = \"admin" not in content
        assert "password = \"123" not in content
        assert "secret_key = \"secret" not in content
