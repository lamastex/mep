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

You may have to change owner to `user` from outside the docker container if:

- the files are created inside the docker container as `root`
- and you want to use git from outside the container as `user`

```
$ sudo chown -R user:group *
```
Replace `user` and `group` with the right values.

# tw

This is the sbt project folder for Twitter processing using Scala-wrapped twitter4j.

```
$ cp tw/src/main/resources/application.conf.template tw/src/main/resources/application.conf  
```
and put you Twitter developer credentials in `tw/src/main/resources/application.conf`.

## sbt

After `sbt compile` you can `run` different main methods like this in side the docker container:
```
root@d9930ca7501d:~/GIT/lamastex/mep/tw# sbt "runMain org.lamastex.mep.tw.getStatusFromID"

# this will stop in 10 seconds = 10000 milliseconds 
root@d9930ca7501d:~/GIT/lamastex/mep/tw# sbt "runMain org.lamastex.mep.tw.StatusStreamer"

# this will be streaming indefinitely
root@d9930ca7501d:~/GIT/lamastex/mep/tw# sbt "runMain org.lamastex.mep.tw.StatusStreamer -1"

root@d9930ca7501d:~/GIT/lamastex/mep/tw# sbt "runMain org.lamastex.mep.tw.getUserTimeline"

# this will stop in 10 seconds = 10000 milliseconds 
root@d9930ca7501d:~/GIT/lamastex/mep/tw# sbt 'runMain org.lamastex.mep.tw.SearchStreamer "TRUMP IS" "BLM" "WLM"' 

# this will be streaming indefinitely
root@d9930ca7501d:~/GIT/lamastex/mep/tw# sbt 'runMain org.lamastex.mep.tw.SearchStreamer -1 "TRUMP IS" "BLM" "ALM"' 

```
