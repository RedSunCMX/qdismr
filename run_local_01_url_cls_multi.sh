#!/bin/bash
export LANG=en_US.UTF-8

alias python='/home/chenmingxing/software/epd-7.1-2-rh3-x86_64/bin/python'

HADOOP_HOME=/home/work/software/hadoop

pfix=star_01_url_cls_run_local
YYYYMMDD=20131123

INPUT=/home/nlp/mingxing/wordcluster/qStat/star_00_filt/${YYYYMMDD}/0000/
OUTPUT=/home/nlp/mingxing/wordcluster/qStat/${pfix}/${YYYYMMDD}/0000/

sudo -u nlp hadoop fs -rmr ${OUTPUT}
sudo -u nlp hadoop fs -mkdir ${OUTPUT}

# for f in `sudo -u nlp hadoop fs -ls /home/nlp/mingxing/wordcluster/qStat/star_00_filt/20131123/0000/ | awk -F" " '{if($NF != "items")print $NF}'`;
for n in `seq 1 1 9`;
do
    n=`printf "%05d" $n`
    f="/home/nlp/mingxing/wordcluster/qStat/star_00_filt/20131123/0000/part-$n"
    echo "hello:" ${f}
    partNo=`echo ${f} | awk -F"/" '{if($NF != "_SUCCESS")print $NF}'`
    echo "partNo:" ${partNo}
    if [ ${partNo} == "_SUCCESS" ] || [ ${partNo} == "_temporary" ] || [ ${partNo} == "items" ]; then
        continue
    fi
    echo "partNo:" ${partNo}
    sudo -u nlp hadoop fs -cat ${f} | python star_01_url_cls_mapper.py > url_cls_local_${partNo}
    sudo -u nlp hadoop fs -put url_cls_local_${partNo} ${OUTPUT}
    rm -rf url_cls_local_${partNo}
done

