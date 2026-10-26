output "ecs_cluster_arn" {
  description = "ARN of the ECS Fargate cluster"
  value       = aws_ecs_cluster.main.arn
}

output "task_definition_arn" {
  description = "ARN of the ECS Fargate task definition"
  value       = aws_ecs_task_definition.pipeline.arn
}
