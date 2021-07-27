#!/bin/bash
rm -f /root/GIT/sc/tw/tmp/full/placeholder &&
cd /root/GIT/sc/tw &&
sbt "runMain org.lamastex.mep.tw.ThreadedTwitterStreamWithWrite /root/GIT/sc/tw/src/test/resources/streamConfigTTTtest.conf" &&
cat /root/GIT/sc/tw/tmp/test_twitter4j*.jsonl | head -10 > /root/GIT/sc/tw/src/test/resources/test_twitter4j.jsonl &&
rm -f /root/GIT/sc/tw/tmp/full/*.jsonl &&
rm -f /root/GIT/sc/tw/tmp/*.jsonl
