https://github.com/behavioral-ds/evently

## Install history

sudo apt install r-base-core
=======

First we build and run our Docker image from the Dockerfile.

``` 
$ docker build -t lamastex/r-base-evently:latest . 

$ docker run --rm -it --name=evently --mount type=bind,source=${PWD},destination=/root/GIT lamastex/r-base-evently
```
Now inside of the Docker containter, run ```R```, and inside ```R```  run the following to install all dependencies:

``` 
$ R

$ if (!require('devtools')) install.packages('devtools')
$ devtools::install_github('behavioral-ds/evently')

$ library(evently)
$ install.packages('datasets.load')
 ```
 Now with all the depenedencies installed, we want to save the running Docker containter as an image, using ```docker commit```. We do this outside of Docker (but with the Container still running):
 
```
$ docker ps
CONTAINER ID        IMAGE                     COMMAND                  CREATED             STATUS              PORTS               NAMES
06a7d4fd11ab        lamastex/r-base-evently   "/bin/sh -c /bin/bash"   13 minutes ago      Up 13 minutes                           evently
 
$ docker commit 06a7d4fd11ab lamastex/r-base-evently
```
Now we can run the image, and libraries will be installed.
```
$ docker run --rm -it --name=evently --mount type=bind,source=${PWD},destination=/root/GIT lamastex/r-base-evently
```
 
 
