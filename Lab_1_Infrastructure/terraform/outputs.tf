output "account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}

output "vpc_id" {
  description = "ID створеної VPC"
  value       = aws_vpc.main.id
}

output "public_subnet_id" {
  description = "ID публічної підмережі"
  value       = aws_subnet.public.id
}

output "private_subnet_id" {
  description = "ID приватної підмережі"
  value       = aws_subnet.private.id
}

output "internet_gateway_id" {
  description = "ID Internet Gateway"
  value       = aws_internet_gateway.igw.id
}

output "readonly_user_name" {
  description = "Ім'я read-only IAM користувача"
  value       = aws_iam_user.readonly_user.name
}

output "readonly_role_arn" {
  description = "ARN read-only ролі для sts:AssumeRole"
  value       = aws_iam_role.readonly_role.arn
}

output "readonly_policy_arn" {
  description = "ARN read-only політики"
  value       = aws_iam_policy.readonly_policy.arn
}
