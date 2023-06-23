#!/bin/sh

log_file=$1
shift



echo "Running client jar..." > /proc/1/fd/1

echo "cmd is: java -jar client.jar $@" > /proc/1/fd/1

echo "log file is ${log_file}" > /proc/1/fd/1

#java -jar client.jar "$@" > /proc/1/fd/1 2>&1
java -cp client.jar site.ycsb.Client -t -s -P config.properties "$@" 2> ${log_file}.err 1> ${log_file}.out
#java -cp client.jar site.ycsb.Client -t -s -P config.properties "$@" > /proc/1/fd/1 2>&1


#| tee ${log_file}
echo "Goodbye" > /proc/1/fd/1
