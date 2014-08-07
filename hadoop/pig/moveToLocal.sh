#!/bin/sh
#copy data to local
hadoop fs -copyToLocal /tmp/wangwei/data/ ../
echo 'finish copy Hadoop Dir: /tmp/wangwei/data/ to Local Dir: ../'
#clean data
hadoop fs -rmr -skipTrash /tmp/wangwei/data/
echo 'finish clean Hadoop Dir: /tmp/wangwei/data/'