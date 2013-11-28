#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import os, sys, time
import starHadoopUtil as hu
import starConf
import starSCUtils

# -e UV
def mapper1():
    # UV or PV
    opt_str = sys.argv[2]
    for line in sys.stdin:
        # query UV PV uid timestamp date query url title refer_url
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 10:
            continue
        cnt = 0
        if opt_str == "UV":
            cnt = int(words[1].split(":")[-1])
        elif opt_str == "PV":
            cnt = int(words[2].split(":")[-1])

        if cnt >= 100:
            print "\t".join(words)

# -e K
def reducer1():

    K = int(sys.argv[2])
    sc = starSCUtils.spectralClusU()

    def newQ(ws):
        logs = []
        logs.append(ws[3:])
        return ws, logs 

    def printQ(logs):
        sc.sc_exec(logs, K)
        # for i in xrange(len(logs)):
        #    # query uv pv uid timestamp date query url title refer_url
        #    print "\t".join([logs[i][0], "UV:%d"%(len(uidS)), \
        #      "PV:%d"%(len(logs)), "\t".join(logs[i][1:])])

    lastWords = []
    words = []
    logsInQ = []
    for line in sys.stdin:
        line = line.strip()
        # query UV PV uid timestamp date query url title refer_url
        words = [d.strip() for d in line.split("\t")]
        if len(words) != 10:
            continue
        if len(lastWords) == 0:
            lastWords,logsInQ = newQ(words)
            continue
        if words[0] != lastWords[0]:
            printQ(logsInQ)
            lastWords,logsInQ = newQ(words)
            continue
        logsInQ.append(words[3:])
        lastWords = words

    printQ(logsInQ)
 
def setupIO(input):
    # YYYYMMDD dayNum UV|PV K 
    if len(input) != 4:
        return -1
    hdp.input = []
    dayNum = input[1]
    t1 = time.mktime(time.strptime(input[0], "%Y%m%d"))
    for i in range(int(dayNum)):
        tstr = time.strftime("%Y%m%d", time.localtime(t1 - i*24*3600))
        # path = starConf.wdInputH + tstr
        # path = "/home/nlp/mingxing/ST00UidSort/" + tstr + starConf.wdOutputE
        path = starConf.wcOutput + "ST01QuerySort/" + tstr + starConf.wdOutputE
        # hdp.input.append(path)
        print path
        cmd = starConf.HadoopPath + " fs -test -e " + path
        if os.system(cmd) == 0:
            hdp.input.append(path)
        # else:
        #    return -1
    # output path
    # hdp.output=starConf.wcOutput + input[0]  + "/ST00UidSort" + starConf.wdOutputE
    # hdp.output="/home/nlp/mingxing/ST01QuerySort/" + input[0] + starConf.wdOutputE
    hdp.output=starConf.wcOutput + "ST02URLCLS/" + input[0] + starConf.wdOutputE

    # source
    hdp.mapPara=[]
    hdp.mapPara.append(input[2])
    hdp.reducePara=[]
    hdp.reducePara.append(input[3])
    hdp.projectName = hdp.projectName + ":" + input[0]
    return 0

def main():
    hdp.projectName="chenmingxing_02_url_sc"
    hdp.mapper=mapper1
    hdp.reducer=reducer1
    # hdp.reducer="NONE"
    hdp.mainFile=sys.argv[0]
    hdp.mapCapacity=starConf.mapCapacity
    hdp.reduceCapacity=starConf.reduceCapacity
    hdp.reduceTask=starConf.reduceTask
    hdp.fileL=["*.py"]
    hdp.otherD=[]
    hdp.otherD.append("mapred.map.over.capacity.allowed=false")
    hdp.otherD.append("mapred.reduce.over.capacity.allowed=false")
    hdp.otherD.append("stream.memory.limit=9216")
    # hdp.otherD.append("mapred.min.split.size=999999999999")
    # hdp.otherD.append("mapred.compress.map.output=true")
    # hdp.otherD.append("mapred.map.output.compression.codec=org.apache.hadoop.io.compress.LzmaCodec")
    # hdp.otherD.append("mapred.output.compress=true")
    # hdp.otherD.append("mapred.output.compression.codec=org.apache.hadoop.io.compress.LzmaCodec")
    hdp.keyFields=5
    hdp.partitioner="Keybase"
    hdp.partitionFields=1
    hdp.smsAlarm=starConf.phoneNum
    hdp.customFun=setupIO
    hdp.execute(sys.argv)

# python urlM_01_filter.py -e $YYYYMMDD(截止日期) -e $DAYNUM(往前天数) -e $src -r
if __name__ == "__main__":
    hdp=hu.job()
    sys.exit(main())

