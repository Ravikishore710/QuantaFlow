variable "aws_region" {
  description = "Target AWS region for infrastructure deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project identifier used in naming and resource tagging"
  type        = string
  default     = "quantaflow"
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "db_instance_class" {
  description = "RDS PostgreSQL database compute class"
  type        = string
  default     = "db.t4g.medium"
}

variable "alert_webhook_url" {
  description = "External webhook URL for Write-Audit-Publish data quality alert dispatch"
  type        = string
  default     = ""
  sensitive   = true
}
