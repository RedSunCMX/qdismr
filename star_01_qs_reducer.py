#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import sys

def printQC(query, uv, pv):
    if uv >= 100:            
        print "\t".join([query, "UV:%d"%(uv), "PV:%d"%(pv)])

def main(argv):

    uv,pv = 0,0
    lastWords,words = [],[]
    for line in sys.stdin:
        # query uid timestamp date query url title refer_url
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 8:
            continue
        if len(lastWords) == 0:
            lastWords,uv,pv = words,1,1
            continue

        if lastWords[0] != words[0]:
            printQC(lastWords[0], uv, pv)
            lastWords,uv,pv = words,1,1
            continue
            
        pv += 1
        # uv
        if words[1] != lastWords[1]:
            uv += 1
        lastWords = words

    printQC(lastWords[0], uv, pv)

    return 0
 
if __name__ == "__main__":
    sys.exit(main(sys.argv))

