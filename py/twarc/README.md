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

Set the environment variables in file named `env.list` with your Twitter Developer Credentials:

```
$ cp env.list.template env.list
$ vi env.list
```

Run `twarc` in command-line inside a mounted docker container named `mep-pytwarc` in interactive mode after starting it in daemon mode with environment variables in `env.list` file as follows:

```
docker pull lamastex/python-twarc:latest
docker run --rm -d -it --name=mep-pytwarc --mount type=bind,source=${PWD},destination=/root/GIT --env-file env.list lamastex/python-twarc:latest
$ docker exec -it mep-pytwarc /bin/bash
```

