#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import os, sys, time
import starHadoopUtil as hu
import starConf

def mapper1():
    for line in sys.stdin:
        # uid timestamp date query url title refer_url
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 7:
            continue
        print "\t".join([words[3]] + words)

def reducer1():
    def newQ(ws):
        logs = []
        logs.append(ws)
        uidS = set()
        uidS.add(ws[1])
        return ws, logs, uidS 

    def printQ(logs, uidS):
        for i in xrange(len(logs)):
            # print "\t".join([logs[i][0], "UV:%d"%(len(uidS)), "PV:%d"%(len(logs))] + logs[i][1:])
            print "\t".join([logs[i][0], "UV:%d"%(len(uidS)), "PV:%d"%(len(logs)), "\t".join(logs[i][1:])])

    lastWords = []
    words = []
    logsInQ = []
    uS = set()
    for line in sys.stdin:
        line = line.strip()
        # query uid timestamp date query url title refer_url
        words = [d.strip() for d in line.split("\t")]
        if len(words) != 8:
            continue
        if len(lastWords) == 0:
            lastWords, logsInQ, uS = newQ(words)
            continue
        if words[0] != lastWords[0]:
            if len(uS) >= 100:
                printQ(logsInQ, uS)
            del logsInQ
            del uS
            # if len(uS) >= 100:
            #    printQ(logsInQ)
            lastWords, logsInQ, uS = newQ(words)
            continue
        logsInQ.append(words)
        uS.add(words[1])
        lastWords = words

    if len(uS) >= 100:
        printQ(logsInQ, uS)
    del logsInQ
    del uS
    # if len(uS) >= 100:
    #    printQ(logsInQ)
 
def setupIO(input):
    if len(input) != 2:
        return -1
    hdp.input = []
    dayNum = input[1]
    t1 = time.mktime(time.strptime(input[0], "%Y%m%d"))
    for i in range(int(dayNum)):
        tstr = time.strftime("%Y%m%d", time.localtime(t1 - i*24*3600))
        # path = starConf.wdInputH + tstr + "/Inc.slog10.safe.zzbc.qihoo.net.Inc.*"
        # path = starConf.wdInputH + tstr
        path = "/home/nlp/mingxing/ST00UidSort/" + tstr + starConf.wdOutputE
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
    hdp.output=starConf.wcOutput + "ST01QuerySort/" + input[0] + starConf.wdOutputE

    # source
    #hdp.mapPara=[]
    #hdp.mapPara.append(input[2])
    hdp.projectName = hdp.projectName + ":" + input[0]
    return 0

def main():
    hdp.projectName="chenmingxing_01_query_sort"
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
    hdp.otherD.append("io.sort.mb=100")
    # hdp.otherD.append("mapred.min.split.size=999999999999")
    # hdp.otherD.append("mapred.compress.map.output=true")
    # hdp.otherD.append("mapred.map.output.compression.codec=org.apache.hadoop.io.compress.LzmaCodec")
    # hdp.otherD.append("mapred.output.compress=true")
    # hdp.otherD.append("mapred.output.compression.codec=org.apache.hadoop.io.compress.LzmaCodec")
    hdp.keyFields=2
    hdp.partitioner="Keybase"
    hdp.partitionFields=1
    hdp.smsAlarm=starConf.phoneNum
    hdp.customFun=setupIO
    hdp.execute(sys.argv)

# python urlM_01_filter.py -e $YYYYMMDD(截止日期) -e $DAYNUM(往前天数) -e $src -r
if __name__ == "__main__":
    hdp=hu.job()
    sys.exit(main())

