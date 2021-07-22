data "template_file" "iam_policy" {
  template = file("${path.module}/policies/iam_policy.json")
  vars = {
    s3-bucket = file("${path.module}/credentials/aws-s3-bucket")
  }
}

data "template_file" "instance_startup_script" {
  template = file("${path.module}/cloud-init.yml")
  vars = {
    twitter-credentials = "${path.module}/credentials/application.conf"
    github-credentials  = "${path.module}/credentials/github-ssh"
    known-hosts         = "${path.module}/resources/known_hosts"
    docker-start        = "${path.module}/resources/docker-start.sh"
    config-repo         = file("${path.module}/credentials/config-repository")
    config-repo-owner   = file("${path.module}/credentials/config-repo-owner")
  }
}