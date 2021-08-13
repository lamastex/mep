#!/bin/bash
root_dir=$1
rm -f $root_dir/tmp/full/placeholder &&
cd $root_dir/ &&
sbt "runMain org.lamastex.mep.tw.ThreadedTwitterStreamWithWrite $root_dir/src/test/resources/streamConfigTTTtest.conf" &&
cat $root_dir/tmp/test_twitter4j*.jsonl | head -10 > $root_dir/src/test/resources/test_twitter4j.jsonl &&
rm -f $root_dir/tmp/full/*.jsonl &&
rm -f $root_dir/tmp/*.jsonl
