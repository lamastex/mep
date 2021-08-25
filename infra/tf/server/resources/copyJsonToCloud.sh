#!/bin/bash

# $1: directory with .jsonl to compress
# $2: directory to temporarily keep files before copy to s3
# $3: address to s3 bucket

JSONLDIR=$1
TOCLOUDDIR=$2
S3BUCKET=$3

# Control if the files are moved or copied from their original location.
# Files will be deleted after being copied to S3, so if the original files
# should still be available, use cp instead of mv.
TRANSFER=mv

# Wait this many seconds between operations.
# Useful for seeing each step in action.
# Set to 0 to disable (no waiting between operations).
DEBUGSLEEP=0

# Flags for $ aws s3 cp
AWSFLAGS="--recursive"


$TRANSFER $JSONLDIR/*.jsonl $TOCLOUDDIR && sleep $DEBUGSLEEP && \
  cd $TOCLOUDDIR && sleep $DEBUGSLEEP && \
  bzip2 -k ./*.jsonl && sleep $DEBUGSLEEP && \
  mkdir compressed && sleep $DEBUGSLEEP && \
  mv *.bz2 compressed/ && sleep $DEBUGSLEEP && \
  sudo aws s3 cp $AWSFLAGS compressed/ $S3BUCKET  && sleep $DEBUGSLEEP && \
  rm compressed/*.bz2 && sleep $DEBUGSLEEP && \
  rmdir compressed && sleep $DEBUGSLEEP && \
  rm *.jsonl