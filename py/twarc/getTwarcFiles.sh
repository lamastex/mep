#!/bin/bash
twarc hydrate /root/GIT/sc/tw/work/tweetIDs.txt > /root/GIT/py/twarc/test_twarc.jsonl && 
mv /root/GIT/py/twarc/test_twarc.jsonl /root/GIT/sc/tw/src/test/resources/test_twarc.jsonl
