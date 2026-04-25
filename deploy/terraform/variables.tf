variable "project" {
  description = "Project/name prefix for all resources."
  type        = string
  default     = "retail-lakehouse"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "data_bucket_name" {
  description = "Globally unique S3 bucket name for the data lake."
  type        = string
}
