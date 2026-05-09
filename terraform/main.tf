# Configure the AWS provider
# Terraform uses this to know which cloud and region to talk to
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Store Terraform state in S3 so it persists and can be shared
  # Comment this out for first run — bootstrap problem (can't store state
  # in a bucket that doesn't exist yet)
  # backend "s3" {
  #   bucket = "shipment-tracking-terraform-state"
  #   key    = "state/terraform.tfstate"
  #   region = "eu-central-1"
  # }
}

provider "aws" {
  region = var.aws_region
}