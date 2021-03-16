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

-  The files are created inside the docker container as `root`
- and you want to use git from outside the container as `user`

```
$ sudo chown -R user:group *
```
Replace `user` and `group` with the right values.

stop the docker container:
```
$ docker stop mep
```

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
# this will stream indefinitely and pass JSON lines of tweets to 
sbt -error 'runMain org.lamastex.mep.tw.SearchStreamer -1 "TRUMP IS" "BLM" "ALM"' 2> work/errors.log | grep '^{.*}$' - 1> work/out

# followIDs Streamer will stop in 10 seconds
root@7bd50f3f9461:~/GIT/lamastex/mep/tw# sbt 'runMain org.lamastex.mep.tw.FollowIdsStreamer 10000'

# followIDs Streamer will stream indefinitely
root@7bd50f3f9461:~/GIT/lamastex/mep/tw# sbt 'runMain org.lamastex.mep.tw.FollowIdsStreamer -1'

# followIDs Streamer will stream indefinitely following the IDs after -1
root@7bd50f3f9461:~/GIT/lamastex/mep/tw# sbt 'runMain org.lamastex.mep.tw.FollowIdsStreamer -1 1344951L 3108351L'

# location streamer indefinitely for the default bounding boxes for Sweden and Finland
root@7bd50f3f9461:~/GIT/lamastex/mep/tw# sbt 'runMain org.lamastex.mep.tw.LocationStreamer -1 '

# location streamer indefinitely for India: https://gist.github.com/graydon/11198540
sbt 'runMain org.lamastex.mep.tw.LocationStreamer -1 68.1766451354 7.96553477623 97.4025614766 35.4940095078'
```

# References

## Libraries used

- twitter4j
  - https://github.com/Twitter4J/Twitter4J
  - http://twitter4j.org/en/code-examples.html
  - https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/
- os-lib
  - https://github.com/com-lihaoyi/os-lib

## Libraries for potential use:

- https://github.com/apache/bahir
  - https://github.com/apache/bahir/tree/master/streaming-twitter
