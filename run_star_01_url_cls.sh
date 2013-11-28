#!/bin/bash
export LANG=en_US.UTF-8
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./epd.tar.gz/epd-7.1-2-rh3-x86_64/lib/libpython2.7.so
# alias python='/home/chenmingxing/software/epd-7.1-2-rh3-x86_64/bin/python'

HADOOP_HOME=/home/work/software/hadoop

pfix=star_01_url_cls
YYYYMMDD=20131123

INPUT=/home/nlp/mingxing/wordcluster/qStat/star_00_filt/${YYYYMMDD}/0000/
OUTPUT=/home/nlp/mingxing/wordcluster/qStat/${pfix}/${YYYYMMDD}/0000/

EPDJAR=/home/chenmingxing/software/epd.jar
# sudo -u nlp hadoop fs -rmr /home/nlp/mingxing/wordcluster/bin/epd.tar.gz
# sudo -u nlp hadoop fs -put /home/chenmingxing/software/epd.tar.gz /home/nlp/mingxing/wordcluster/bin/
# EPDJAR=/home/nlp/mingxing/wordcluster/bin/epd.tar.gz

JAR=hadoop${pfix}.jar
rm ${JAR}
jar cvf ${JAR} ${pfix}_*.py
chmod 755 ${JAR}

sudo -u nlp hadoop fs -rmr ${OUTPUT}
sudo -u nlp hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming.jar \
  -archives "hdfs:///home/nlp/mingxing/wordcluster/bin/epd.tar.gz"           \
  -D mapred.job.name=chenmingxing_${pfix}                                    \
  -D mapred.fairscheduler.pool=nlp                                           \
  -D mapred.job.priority=NORMAL                                              \
  -D mapred.job.map.capacity=500                                             \
  -D mapred.job.reduce.capacity=500                                          \
  -D mapred.map.tasks=500                                                    \
  -D mapred.reduce.tasks=500                                                 \
  -D mapred.map.over.capacity.allowed=false                                  \
  -D mapred.reduce.over.capacity.allowed=false                               \
  -D mapred.min.split.size=999999999999                                      \
  -D stream.num.map.output.key.fields=2                                      \
  -D num.key.fields.for.partition=1                                          \
  -D io.sort.mb=100                            \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner         \
  -input ${INPUT}                                                            \
  -output ${OUTPUT}                                                          \
  -file ${pfix}_mapper.py                                                    \
  -file ${pfix}_utils.py                                                     \
  -file run_set_env.sh                                                       \
  -mapper "sh run_set_env.sh"                                                \
  -reducer "cat"

# -mapper "ls -al epd.tar.gz/ >&2"    \
# -mapper "usr.tar.gz/usr/bin/python2.7 ${pfix}_mapper.py"            \
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
