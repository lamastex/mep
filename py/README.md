
This is for python dev environemnt for the following packages:

- [twarc](https://github.com/DocNow/twarc)
- [mep](https://github.com/lamastex/mep/py)

See https://github.com/lamastex/dockerDev for details on docker builds.

To Use:

In interactive mode after starting daemon mode with environment variables in `env.list` file.

```
docker pull lamastex/python-twarc:latest
docker run --rm -d -it --name=mep-pytwarc --mount type=bind,source=${PWD},destination=/root/GIT --env-file env.list lamastex/python-twarc:latest
$ docker exec -it mep-pytwarc /bin/bash
```

