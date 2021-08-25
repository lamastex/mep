#!/bin/bash
docker pull lamastex/dockerdev:spark3x

docker run --name mep-dev --restart=always --workdir="/root/work-dir" --mount type=bind,source=/home/ubuntu,destination=/root/work-dir --entrypoint ./docker-start.sh lamastex/dockerdev:spark3x
