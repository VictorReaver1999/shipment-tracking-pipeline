# AWS region to deploy resources into
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-central-1"  # Frankfurt — closest to Hamburg
}

# Project name used for naming and tagging resources
variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "shipment-tracking"
}

# Environment tag — useful when you have dev/staging/prod
variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}