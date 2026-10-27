# QuantaFlow Infrastructure-as-Code (Terraform)

This directory contains enterprise-grade Terraform configurations for deploying the **QuantaFlow** lakehouse architecture to AWS.

---

## 1. Architecture & Module Design

The deployment is split into three focused, least-privilege modules:

- **`modules/storage`**:
  - AWS S3 Lakehouse Bucket with AES-256 / KMS customer-managed key encryption.
  - S3 bucket versioning and automated lifecycle policy (Glacier transition after 90 days).
  - Strict Public Access Block (`block_public_acls = true`, `block_public_policy = true`, `ignore_public_acls = true`, `restrict_public_buckets = true`).

- **`modules/warehouse`**:
  - Production RDS PostgreSQL 16 analytical warehouse instance in isolated private subnets.
  - Storage-level encryption enabled via AWS KMS (`storage_encrypted = true`).
  - Automated random password generation with credentials managed and rotated via AWS Secrets Manager.
  - Zero-public ingress security group restricted to private VPC CIDR (`10.0.0.0/16`).

- **`modules/orchestration`**:
  - AWS ECS Fargate cluster with Container Insights enabled.
  - Customer-managed KMS key for encrypting CloudWatch log groups with 90-day retention.
  - Container health check integration probing `/health` and `/readyz` endpoints.

---

## 2. Remote State & Encryption Security

State is persisted in an encrypted, distributed backend:

```hcl
terraform {
  backend "s3" {
    bucket         = "quantaflow-terraform-state"
    key            = "prod/quantaflow.tfstate"
    region         = "us-east-1"
    encrypt        = true
    kms_key_id     = "alias/quantaflow-tf-state"
    dynamodb_table = "quantaflow-terraform-locks"
  }
}
```

- **Encryption at Rest**: AES-256 via customer-managed KMS key.
- **Concurrency Locking**: DynamoDB table prevents conflicting deployments.

---

## 3. Provider Pinning & Security Scanning

All providers are pinned to compatible minor releases:
- `aws`: `~> 5.40.0`
- `random`: `~> 3.6.0`

Security policy conformance is validated in CI using `aquasecurity/tfsec-action` with zero high/critical severity findings.

---

## 4. Local Execution & Verification

```bash
# 1. Initialize backend and modules
terraform init -backend=false

# 2. Check formatting
terraform fmt -check -recursive

# 3. Validate configuration
terraform validate

# 4. Generate speculative execution plan
terraform plan -lock=false -input=false -out=tfplan \
  -var="environment=ci" \
  -var="aws_region=us-east-1" \
  -var="project_name=quantaflow"
```
