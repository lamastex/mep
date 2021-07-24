#!/bin/bash

# Moves all but the last file (ordered by ls) to another directory.
# $1: source directory (full path)
# $2: destination directory (full path)

files_to_move=$(ls -d $1* | head -n $(($(ls $1|wc -l) - 1)))
echo $files_to_move
if ! test -z "$files_to_move"
then
	mv $files_to_move $2
fi
