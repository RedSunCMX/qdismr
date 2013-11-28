#!/bin/bash

HADOOP_HOME=/home/work/software/hadoop

pfix=star_00_qsort
YYYYMMDD=20131126

#INPUT=/home/maintable/data/v2token_filter/*/Inc*
INPUT=/home/nlp/zhouwei/hadoopapp/test/querycluster/QuerySessionJoinByDateTime_out                                           
#INPUT=/home/nlp/mingxing/ST00UidSort/${YYYYMMDD}/0000/part-*
OUTPUT=/home/nlp/mingxing/wordcluster/qStat/${pfix}/${YYYYMMDD}/0000/

JAR=hadoop${pfix}.jar
rm ${JAR}
jar cvf ${JAR} ${pfix}_*.py
chmod 755 ${JAR}

sudo -u nlp hadoop fs -rmr ${OUTPUT}
sudo -u nlp hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming.jar \
  -archives ${JAR}                             \
  -D mapred.job.name="chenmingxing_${pfix}"    \
  -D mapred.job.priority=NORMAL                \
  -D mapred.job.map.capacity=500               \
  -D mapred.job.reduce.capacity=500            \
  -D mapred.map.tasks=500                      \
  -D mapred.reduce.tasks=500                   \
  -D mapred.map.over.capacity.allowed=false    \
  -D mapred.reduce.over.capacity.allowed=false \
  -D stream.num.map.output.key.fields=3        \
  -D num.key.fields.for.partition=1            \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner  \
  -input ${INPUT}                              \
  -output ${OUTPUT}                            \
  -mapper "python ${JAR}/${pfix}_mapper.py"    \
  -reducer "python ${JAR}/${pfix}_reducer.py"

# -reducer "cat"
# -D mapred.min.split.size=50000000            \
# -D stream.memory.limit=9216                  \
# -D io.sort.mb=100                            \
