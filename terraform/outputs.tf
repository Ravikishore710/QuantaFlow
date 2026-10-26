output "lakehouse_bucket_arn" {
  description = "ARN of the primary S3 lakehouse storage bucket"
  value       = module.storage.lakehouse_bucket_arn
}

output "lakehouse_bucket_name" {
  description = "Name of the primary S3 lakehouse storage bucket"
  value       = module.storage.lakehouse_bucket_name
}

output "rds_postgres_endpoint" {
  description = "Connection endpoint hostname for RDS PostgreSQL 16"
  value       = module.warehouse.rds_endpoint
}

output "database_secret_arn" {
  description = "ARN of AWS Secrets Manager secret storing PostgreSQL credentials"
  value       = module.warehouse.secret_arn
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS Fargate cluster"
  value       = module.orchestration.ecs_cluster_arn
}

output "pipeline_task_definition_arn" {
  description = "ARN of the ECS task definition running QuantaFlow pipelines"
  value       = module.orchestration.task_definition_arn
}
