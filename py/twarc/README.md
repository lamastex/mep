# mep/py
Project MEP: Meme Evolution programme. A python toolkit to do statistical experiments in Twitter. 

## mep/py/twarc
Project MEP: Meme Evolution programme. A python toolkit to do statistical experiments in Twitter with twarc 

### dev
This is for python dev environment for the following packages:

- [twarc](https://github.com/DocNow/twarc)
- [mep](https://github.com/lamastex/mep/py/twarc)

See https://github.com/lamastex/dockerDev for details on docker builds.

### Use

In interactive mode after starting daemon mode with environment variables in `env.list` file.

```
docker pull lamastex/python-twarc:latest
docker run --rm -d -it --name=mep-pytwarc --mount type=bind,source=${PWD},destination=/root/GIT --env-file env.list lamastex/python-twarc:latest
$ docker exec -it mep-pytwarc /bin/bash
```

