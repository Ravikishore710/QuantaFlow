output "lakehouse_bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}

output "lakehouse_bucket_name" {
  value = aws_s3_bucket.lakehouse.id
}
