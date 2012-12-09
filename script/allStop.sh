#! /usr/bin/env bash

# load settings
source `dirname $0$`/config.sh

for ((i=1; i<= 9;i++));do	
	echo ssh ${NODE_PREFIX}${i} "pkill java";
	ssh ${NODE_PREFIX}${i} "pkill java";
done

#for((i=1; i <= 4 ; i++));do done
