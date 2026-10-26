resource "random_password" "db_password" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "${var.project_name}/${var.environment}/postgres_credentials"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "quantaflow"
    password = random_password.db_password.result
    dbname   = "quantaflow"
  })
}

resource "aws_db_subnet_group" "warehouse" {
  name       = "${var.project_name}-${var.environment}-db-subnets"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-${var.environment}-rds-sg"
  description = "Least-privilege security group for PostgreSQL analytical warehouse"
  vpc_id      = var.vpc_id

  ingress {
    description = "PostgreSQL access from private VPC subnets"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_db_instance" "warehouse" {
  identifier              = "${var.project_name}-${var.environment}-postgres"
  engine                  = "postgres"
  engine_version          = "16.2"
  instance_class          = var.db_instance_class
  allocated_storage       = 50
  max_allocated_storage   = 500
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = "quantaflow"
  username                = "quantaflow"
  password                = random_password.db_password.result
  db_subnet_group_name    = aws_db_subnet_group.warehouse.name
  vpc_security_group_ids  = [aws_security_group.rds.id]
  skip_final_snapshot     = true
  backup_retention_period = 14
  deletion_protection     = false
}
