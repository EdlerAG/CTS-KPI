data "aws_caller_identity" "current" {}

# 1. Другий IAM-користувач (без адмін-прав)
resource "aws_iam_user" "readonly_user" {
  name = var.readonly_username
  path = "/"
}

# 2. Кастомна політика: ЛИШЕ перегляд ресурсів, без права запису/створення/видалення
resource "aws_iam_policy" "readonly_policy" {
  name        = "${var.project_name}-readonly-policy"
  description = "Allows read-only inspection of AWS resources without write permissions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadOnlyDescribeListGet"
        Effect = "Allow"
        Action = [
          "ec2:Describe*",
          "iam:Get*",
          "iam:List*",
          "s3:Get*",
          "s3:List*",
          "cloudwatch:Describe*",
          "cloudwatch:Get*",
          "cloudwatch:List*",
          "logs:Describe*",
          "logs:Get*",
          "logs:FilterLogEvents",
          "budgets:ViewBudget",
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      }
    ]
  })
}

# Прив'язуємо read-only політику до користувача
resource "aws_iam_user_policy_attachment" "readonly_user_attach" {
  user       = aws_iam_user.readonly_user.name
  policy_arn = aws_iam_policy.readonly_policy.arn
}

# 3. IAM-роль із цією ж read-only політикою, яку може прийняти (AssumeRole) readonly_user
resource "aws_iam_role" "readonly_role" {
  name        = "${var.project_name}-readonly-role"
  description = "Read-only role assumable by ${var.readonly_username}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowReadOnlyUserAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_user.readonly_user.arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "readonly_role_attach" {
  role       = aws_iam_role.readonly_role.name
  policy_arn = aws_iam_policy.readonly_policy.arn
}

# Дозволяємо користувачу readonly_user виконувати sts:AssumeRole саме для цієї ролі
resource "aws_iam_policy" "allow_assume_readonly_role" {
  name        = "${var.project_name}-allow-assume-readonly-role"
  description = "Allows ${var.readonly_username} to assume ${var.project_name}-readonly-role"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowAssumeReadOnlyRole"
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.readonly_role.arn
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "readonly_user_assume_attach" {
  user       = aws_iam_user.readonly_user.name
  policy_arn = aws_iam_policy.allow_assume_readonly_role.arn
}
