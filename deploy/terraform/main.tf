# Example infrastructure for the retail lakehouse: an encrypted, versioned S3
# data lake, a Glue catalog database per medallion layer, and a least-privilege
# IAM role for the pipeline. This is a reviewed example, not applied from CI.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# ---------------------------------------------------------------------------
# Encryption key for data at rest
# ---------------------------------------------------------------------------
resource "aws_kms_key" "lake" {
  description             = "${var.project} data lake encryption key"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

# ---------------------------------------------------------------------------
# S3 data lake bucket (bronze/silver/gold live under prefixes)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket = var.data_bucket_name
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Glue catalog: one database per layer
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "layer" {
  for_each = toset(["bronze", "silver", "gold"])
  name     = "${replace(var.project, "-", "_")}_${each.key}"
}

# ---------------------------------------------------------------------------
# Least-privilege IAM role for the pipeline
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.project}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid     = "LakeObjectAccess"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*",
    ]
  }
  statement {
    sid       = "UseLakeKey"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:GetDatabase", "glue:GetPartitions",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "pipeline" {
  name   = "${var.project}-pipeline"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline.json
}
