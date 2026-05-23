# Conscious Cloud - Infrastructure
#
# This Terraform module provisions the AWS infrastructure required
# for the compute arbitrage engine: VPC, ALB, ElastiCache (Redis),
# EventBridge rules, and EC2 instance management roles.
#
# Usage:
#   terraform init
#   terraform plan -var-file=prod.tfvars
#   terraform apply

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# VPC for isolated compute topology
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "conscious-cloud-vpc"
  }
}

# Redis for real-time state management
resource "aws_elasticache_cluster" "state" {
  cluster_id           = "cc-state"
  engine               = "redis"
  node_type            = var.redis_node_type
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  subnet_group_name    = aws_elasticache_subnet_group.main.name
  security_group_ids   = [aws_security_group.redis.id]
}

resource "aws_elasticache_subnet_group" "main" {
  name       = "cc-redis-subnet"
  subnet_ids = aws_subnet.private[*].id
}

# EventBridge rule for EC2 state changes
resource "aws_cloudwatch_event_rule" "ec2_state" {
  name        = "cc-ec2-state-changes"
  description = "Capture EC2 instance state transitions"

  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Instance State-change Notification"]
    detail = {
      state = ["running", "shutting-down", "stopped", "terminated"]
    }
  })
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.ec2_state.name
  arn       = aws_lambda_function.state_handler.arn
}

# Lambda for processing EventBridge events
resource "aws_lambda_function" "state_handler" {
  filename         = "lambda-handler.zip"
  function_name    = "cc-state-handler"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("lambda-handler.zip")

  environment {
    variables = {
      REDIS_ENDPOINT = aws_elasticache_cluster.state.cache_nodes[0].address
    }
  }
}
