#!/bin/bash

# we will get status from IDs
N=$4 # number of parts for split command
input=$1 # textfile of tweet IDs
s3bucket=$2 # s3 bucket you want to copy to
K=$3 # starting split
while [[ $K -le "$N" ]]
do
    rm -f twarc.log &&
    split --number="$K"/"$N" "$input" > IDsToHydrate.txt &&
    docker exec -it -w /root/GIT/mep/py/twarc/work mep-pytwarc-alfred twarc hydrate IDsToHydrate.txt > tweets_${K}_of_${N}.jsonl &&
    gzip tweets_${K}_of_${N}.jsonl &&
    gzip twarc.log &&
    /usr/local/bin/aws s3 cp --quiet tweets_${K}_of_${N}.jsonl.gz s3://${s3bucket}/twitter/hydrated/blm/gz/tweets_${K}_of_${N}.jsonl.gz &&
    /usr/local/bin/aws s3 cp --quiet twarc.log.gz s3://${s3bucket}/twitter/hydrated/blm/gz/logs/twarc_${K}_of_${N}.log.gz &&
    echo "Batch $K of $N from $input gzipped and sent" >> log_of_sent_tweets.txt &&
    rm tweets_${K}_of_${N}.jsonl.gz &&
    rm twarc.log.gz &&
    ((K = K + 1)) 
done
rm -f IDsToHydrate.txt 

