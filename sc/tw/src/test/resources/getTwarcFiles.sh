#!/bin/bash
root_dir=$1
cd $root_dir/ &&
twarc hydrate $root_dir/work/tweetIDs.txt > $root_dir/src/test/resources/test_twarc.jsonl

