#!/bin/bash

HADOOP_HOME=/home/work/software/hadoop

pfix=star_02_qcoocur2
YYYYMMDD=20131123

# INPUT1=/home/nlp/mingxing/test/input/path1
# INPUT2=/home/nlp/mingxing/test/input/path2
# OUTPUT=/home/nlp/mingxing/test/output
INPUT1=/home/nlp/gongyk/query_correlation/query_coocur2
INPUT2=/home/nlp/mingxing/mpi/star_01_url_cls_qnorm/output/output/rank-*
OUTPUT=/home/nlp/mingxing/wordcluster/qStat/${pfix}_v2/${YYYYMMDD}/0000/

JAR=hadoop${pfix}.jar
rm ${JAR}
jar cvf ${JAR} ${pfix}_*.py
chmod 755 ${JAR}

sudo -u nlp hadoop fs -rmr ${OUTPUT}
sudo -u nlp hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming.jar \
  -archives ${JAR}                             \
  -D mapred.job.name="chenmingxing_${pfix}"    \
  -D mapred.job.priority=NORMAL                \
  -D mapred.job.map.capacity=1000              \
  -D mapred.job.reduce.capacity=1000           \
  -D mapred.map.tasks=1000                     \
  -D mapred.reduce.tasks=1000                  \
  -D mapred.map.over.capacity.allowed=false    \
  -D mapred.reduce.over.capacity.allowed=false \
  -D stream.num.map.output.key.fields=3        \
  -D num.key.fields.for.partition=1            \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner  \
  -input ${INPUT1}                             \
  -input ${INPUT2}                             \
  -output ${OUTPUT}                            \
  -mapper "python ${JAR}/${pfix}_mapper.py"    \
  -reducer "python ${JAR}/${pfix}_reducer.py"


#  -reducer "cat"
# -D mapred.min.split.size=50000000            \
# -D stream.memory.limit=9216                  \
# -D io.sort.mb=100                            \
