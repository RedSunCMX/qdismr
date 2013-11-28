#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import sys
    
def newQ(ws):
    logs = []
    logs.append(ws)
    uidS = set()
    uidS.add(ws[1])
    return ws, logs, uidS 

def newQC(ws):
    uidS = set()
    uidS.add(ws[1])
    return ws,uidS,1 

def printQ(logs, uidS):
    if len(uidS) < 100:
        return
    # print "\t".join(logs[0])
    for i in xrange(len(logs)):
        print "\t".join([logs[i][0], "UV:%d"%(len(uidS)), \
          "PV:%d"%(len(logs)), "\t".join(logs[i][1:])])

def printQC(query, uv, pv):
    if pv >= 1000:            
        print "\t".join([query, str(uv), str(pv)])

def main(argv):

    lastWords = []
    words = []
    logsInQ = []
    uS = set()
    pv = 0
    for line in sys.stdin:
        line = line.strip()
        # query uid timestamp date query url title refer_url
        words = [d.strip() for d in line.split("\t")]
        if len(words) != 8:
            continue
        if len(lastWords) == 0:
            # lastWords, logsInQ, uS = newQ(words)
            lastWords,uS,pv = newQC(words)
            continue
        
        if words[0] != lastWords[0]:
            printQC(lastWords[0], len(uS), pv)
            # printQ(logsInQ, uS)
            # del logsInQ
            del uS
            # lastWords,logsInQ,uS = newQ(words)
            lastWords,uS,pv = newQC(words)
            continue

        # logsInQ.append(words)
        uS.add(words[1])
        lastWords = words
        pv += 1

    printQC(lastWords[0], len(uS), pv)
    # printQ(logsInQ, uS)
    # del logsInQ
    del uS

    return 0
 
if __name__ == "__main__":
    sys.exit(main(sys.argv))

