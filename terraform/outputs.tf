# Output the S3 bucket name so it can be referenced by other tools
# e.g. the consumer script can read this to know where to archive events
output "s3_bucket_name" {
  description = "Name of the S3 bucket for shipment event archives"
  value       = aws_s3_bucket.shipment_events.bucket
}

# Output the bucket ARN — needed for IAM policies
# ARN = Amazon Resource Name, a unique identifier for any AWS resource
output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.shipment_events.arn
}

# Output the AWS region
output "aws_region" {
  description = "AWS region resources are deployed in"
  value       = var.aws_region
}