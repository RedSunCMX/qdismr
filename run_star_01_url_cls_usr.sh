#!/bin/bash

export LANG=en_US.UTF-8

alias python='/home/chenmingxing/software/epd-7.1-2-rh3-x86_64/bin/python'

HADOOP_HOME=/home/work/software/hadoop

pfix=star_01_url_cls
YYYYMMDD=20131123

INPUT=/home/nlp/mingxing/wordcluster/qStat/star_00_filt_500/${YYYYMMDD}/0000/
OUTPUT=/home/nlp/mingxing/wordcluster/qStat/${pfix}_500/${YYYYMMDD}/0000/

# EPDJAR=/home/chenmingxing/software/epd.jar
# sudo -u nlp hadoop fs -rmr /home/nlp/mingxing/wordcluster/bin/epd.tar.gz
# sudo -u nlp hadoop fs -put /home/chenmingxing/software/epd.tar.gz /home/nlp/mingxing/wordcluster/bin/
EPDJAR=/home/nlp/mingxing/wordcluster/bin/epd.tar.gz

JAR=hadoop${pfix}.jar
rm ${JAR}
jar cvf ${JAR} ${pfix}_*.py
chmod 755 ${JAR}

sudo -u nlp hadoop fs -rmr ${OUTPUT}
sudo -u nlp hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming.jar \
  -archives hdfs:///home/nlp/mingxing/wordcluster/bin/usr.tar.gz             \
  -D mapred.job.name=chenmingxing_${pfix}                                    \
  -D mapred.fairscheduler.pool=nlp                                           \
  -D mapred.job.priority=NORMAL                                              \
  -D mapred.job.map.capacity=1000                                             \
  -D mapred.job.reduce.capacity=1000                                          \
  -D mapred.reduce.tasks=1000                                                 \
  -D mapred.map.over.capacity.allowed=false                                  \
  -D mapred.reduce.over.capacity.allowed=false                               \
  -D stream.num.map.output.key.fields=1                                      \
  -D num.key.fields.for.partition=1                                          \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner         \
  -input ${INPUT}                                                            \
  -output ${OUTPUT}                                                          \
  -file ${pfix}_mapper.py                                                    \
  -file ${pfix}_utils.py                                                     \
  -mapper "cat"                                                              \
  -reducer "usr.tar.gz/usr/bin/python2.7 ${pfix}_mapper.py"

# -D io.sort.mb=200                                                          \
# -file ${pfix}_mapper.py                                                    \
# -D mapred.min.split.size=999999999999                                      \
#  -file ${pfix}_utils.py                                                     \
# -D stream.memory.limit=10240                                                \
# -D mapred.map.tasks=500                                                    \
# -mapper "ls -al epd.tar.gz/ >&2"    \
# -archives ${JAR}                                                       \
# -mapper "epd.tar.gz/epd-7.1-2-rh3-x86_64/bin/python2.7 ${JAR}/${pfix}_mapper.py"    \
# -mapper "find epd.tar.gz/ >&2"    \
# http://182.118.41.5:50030/jobdetails.jsp?jobid=job_201311251032_151731 
# -archives hdfs:///home/nlp/mingxing/wordcluster/bin/python272.tar.gz             \
# -mapper "usr.tar.gz/usr/bin/python2.7 ${JAR}/${pfix}_mapper.py"    \
#  -archives ${EPDJAR}                                                        \
# -archives /home/nlp/mingxing/wordcluster/bin/epd.tar.gz#epd                \
# -reducer "python ${JAR}/${pfix}_reducer.py"
# -D mapred.min.split.size=50000000            \
# -D stream.memory.limit=9216                  \
# -D io.sort.mb=100                            \
# -cacheArchive /home/nlp/gcl/query/usr.tar.gz                               \
# -mapper "usr.tar.gz/usr/bin/python2.7 star_01_url_cls_mapper.py"           \
