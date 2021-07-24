resource "aws_iam_role" "read_write" {
  name               = "read_write"
  assume_role_policy = file("${path.module}/policies/iam_role.json")
}

resource "aws_iam_instance_profile" "read_write" {
  name = "read_write"
  role = aws_iam_role.read_write.name
}

resource "aws_iam_policy" "policy" {
  name   = "read-write-policy"
  policy = data.template_file.iam_policy.rendered
}

resource "aws_iam_policy_attachment" "attach" {
  name       = "attach"
  roles      = [aws_iam_role.read_write.name]
  policy_arn = aws_iam_policy.policy.arn
}
