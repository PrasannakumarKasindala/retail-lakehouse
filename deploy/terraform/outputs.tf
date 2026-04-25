output "data_bucket" {
  description = "S3 data lake bucket name."
  value       = aws_s3_bucket.lake.bucket
}

output "kms_key_arn" {
  description = "KMS key protecting the data lake."
  value       = aws_kms_key.lake.arn
}

output "glue_databases" {
  description = "Glue catalog database names per layer."
  value       = { for k, db in aws_glue_catalog_database.layer : k => db.name }
}

output "pipeline_role_arn" {
  description = "IAM role assumed by the pipeline."
  value       = aws_iam_role.pipeline.arn
}
