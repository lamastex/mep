# mep
Project MEP: Meme Evolution programme. A scala library to do statistical experiments in Twitter. 

# docker development

```
$ pwd
/home/user/all/git
$ docker run --rm -d -it --name=mep --mount type=bind,source=${PWD},destination=/root/GIT -p 4040:4040 lamastex/dockerdev:latest
d9930ca7501dc470822cc985a4a1180720537929e54413e92ec36760671ae08a
$ docker exec -it mep /bin/bash
root@d9930ca7501d:~# pwd
/root
root@d9930ca7501d:~# cd GIT/lamastex/mep/
```
