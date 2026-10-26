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
