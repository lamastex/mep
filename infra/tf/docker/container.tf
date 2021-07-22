resource "docker_image" "dockerdev" {
  name = "lamastex/dockerdev:spark3x"
  keep_locally = true
}

resource "docker_container" "mep-dev" {
  image = docker_image.dockerdev.latest
  name  = "mep-dev"
  attach = false
  working_dir = "/root/work-dir"
  entrypoint = ["/bin/bash","docker-start.sh"]
  stdin_open = false
  tty = false
  mounts {
    target = "/root/work-dir"
    type = "bind"
    source = "/home/ubuntu"
  }
}
