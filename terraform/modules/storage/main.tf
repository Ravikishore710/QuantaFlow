resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_kms_key" "storage_key" {
  description             = "KMS customer-managed key for QuantaFlow S3 Lakehouse encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${var.project_name}-${var.environment}-lakehouse-${random_id.bucket_suffix.hex}"
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.storage_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "archive-raw-bronze"
    status = "Enabled"

    filter {
      prefix = "data/raw/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}
