terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.14.0"
    }
  }
}

data "terraform_remote_state" "docker_server" {
  backend = "local"
  config = {
    path = "${path.module}/../server/terraform.tfstate"
  }
}

provider "docker" {
  host = "ssh://ubuntu@${data.terraform_remote_state.docker_server.outputs.public_dns}"
}
