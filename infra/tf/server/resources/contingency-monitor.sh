#!/bin/bash
# Moves all but the last file in files-being-written-to to files-complete.
# Necessary since there are cases when the Streaming job misses to move.

inotifywait -m /home/ubuntu/files-being-written-to/ -e create | 
	while read dir action file; do
		sleep 5
		./contingency-move.sh $dir /home/ubuntu/files-complete/
	done
