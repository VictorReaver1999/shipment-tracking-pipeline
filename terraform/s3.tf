# S3 bucket for storing raw shipment event archives
# This is where raw Kafka events get backed up for long-term storage
resource "aws_s3_bucket" "shipment_events" {
  bucket = "${var.project_name}-events-${var.environment}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "${var.project_name}-events"
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }
}

# Block all public access — this bucket is private
# Raw event data should never be publicly accessible
resource "aws_s3_bucket_public_access_block" "shipment_events" {
  bucket = aws_s3_bucket.shipment_events.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning — keeps history of every file uploaded
# Protects against accidental deletion or overwrites
resource "aws_s3_bucket_versioning" "shipment_events" {
  bucket = aws_s3_bucket.shipment_events.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle rule — automatically move old data to cheaper storage
# After 30 days move to STANDARD_IA (infrequent access — cheaper)
# After 90 days move to GLACIER (archive — very cheap, slow retrieval)
resource "aws_s3_bucket_lifecycle_configuration" "shipment_events" {
  bucket = aws_s3_bucket.shipment_events.id

 rule {
    id     = "archive-old-events"
    status = "Enabled"

    filter {
      prefix = ""  # apply to all objects in the bucket
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# Get current AWS account ID — used in bucket name to ensure uniqueness
# S3 bucket names must be globally unique across ALL AWS accounts
data "aws_caller_identity" "current" {}