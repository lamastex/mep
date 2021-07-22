terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }
  required_version = ">= 0.14.9"
}

provider "aws" {
  shared_credentials_file = "${path.module}/credentials/aws-credentials"
  profile = "default"
  region  = "eu-west-1"
}

resource "aws_instance" "ec2_instance" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "t2.small"
  associate_public_ip_address = true
  key_name                    = aws_key_pair.ssh-key.key_name
  vpc_security_group_ids      = [aws_security_group.group.id]
  iam_instance_profile        = aws_iam_instance_profile.read_write.name
  tags = {
    Name = "EC2 Instance"
  }

  user_data = data.template_file.instance_startup_script.rendered
}
