#! /usr/bin/env bash

# load setting
source `dirname $0`/config.sh



echo java -cp ${ANALYZER_CLASSPATH} ${ANALYZER_MAIN_CLASS}  > analyzeLog.txt 2>&1
java -cp ${ANALYZER_CLASSPATH} ${ANALYZER_MAIN_CLASS} 

