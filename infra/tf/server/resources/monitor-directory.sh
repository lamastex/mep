#!/bin/bash
homedir=/home/ubuntu
inotifywait -m $homedir/files-complete/ -e create -e moved_to |
  while read dir action file; do
    ./copyJsonToCloud.sh $dir $homedir/files-to-cloud \
      s3://osint-gdelt-reado/twitter/ingest/$(date +"%Y/%m/%d/") \
  done
