terraform {
  required_version = ">= 1.6.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6.0"
    }
  }

  backend "s3" {
    bucket         = "quantaflow-terraform-state"
    key            = "prod/quantaflow.tfstate"
    region         = "us-east-1"
    encrypt        = true
    kms_key_id     = "alias/quantaflow-tf-state"
    dynamodb_table = "quantaflow-terraform-locks"
  }
}

provider "aws" {
  region                      = var.aws_region
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# Dedicated Networking VPC for QuantaFlow Data Lakehouse and Warehouse
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-${var.environment}-vpc"
  }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 1)
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.project_name}-${var.environment}-public-a"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 10)
  availability_zone = "${var.aws_region}a"

  tags = {
    Name = "${var.project_name}-${var.environment}-private-a"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 11)
  availability_zone = "${var.aws_region}b"

  tags = {
    Name = "${var.project_name}-${var.environment}-private-b"
  }
}

module "storage" {
  source       = "./modules/storage"
  project_name = var.project_name
  environment  = var.environment
}

module "warehouse" {
  source            = "./modules/warehouse"
  project_name      = var.project_name
  environment       = var.environment
  vpc_id            = aws_vpc.main.id
  subnet_ids        = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  db_instance_class = var.db_instance_class
}

module "orchestration" {
  source            = "./modules/orchestration"
  project_name      = var.project_name
  environment       = var.environment
  vpc_id            = aws_vpc.main.id
  subnet_ids        = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  s3_bucket_arn     = module.storage.lakehouse_bucket_arn
  secret_arn        = module.warehouse.secret_arn
  alert_webhook_url = var.alert_webhook_url
}
