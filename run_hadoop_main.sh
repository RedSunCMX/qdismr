#!/bin/bash

INPUT=/home/nlp/mingxing/ST00UidSort/20131118/0000/part-*
OUTPUT=/home/nlp/mingxing/wordcluster/qStat/STOneQ/wutuobang/

jobname="chenmingxing_match"

HADOOP_HOME=/home/work/software/hadoop

jar cvf hadoopMatch.jar mapper.py
chmod 755 *

JAR=hadoopMatch.jar

sudo -u nlp hadoop fs -rmr ${OUTPUT}
sudo -u nlp hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming.jar \
  -archives ${JAR} \
  -D mapred.job.name=$jobname \
  -D mapred.job.priority=NORMAL \
  -D mapred.map.tasks=20000 \
  -D mapred.min.split.size=50000000 \
  -D mapred.job.reduce.capacity=10 \
  -D mapred.reduce.tasks=10 \
  -input ${INPUT} \
  -output ${OUTPUT} \
  -mapper "python ${JAR}/mapper.py" \
  -reducer "cat"


