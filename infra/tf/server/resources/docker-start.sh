#!/bin/bash
cd twitter-scala
sbt "runMain org.lamastex.mep.tw.ThreadedTwitterStreamWithWrite ../twitter-config/twitter-stream.conf"