variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "s3_bucket_arn" {
  type = string
}

variable "secret_arn" {
  type = string
}

variable "alert_webhook_url" {
  type    = string
  default = ""
}
