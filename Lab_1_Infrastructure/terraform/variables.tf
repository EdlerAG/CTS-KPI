variable "aws_region" {
  description = "AWS region where resources will be deployed"
  type        = string
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Project prefix for resource names and tags"
  type        = string
  default     = "kpi-kv-store"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the public subnet"
  type        = string
  default     = "10.0.1.0/24"
}

variable "private_subnet_cidr" {
  description = "CIDR block for the private subnet"
  type        = string
  default     = "10.0.2.0/24"
}

variable "readonly_username" {
  description = "IAM username for the read-only user"
  type        = string
  default     = "kv-readonly-user"
}
