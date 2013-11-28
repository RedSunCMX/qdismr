#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import time, sys
import base64

def newQ(words):
    qset = set([words[1]])
    # print "[DEBUG]query:\n" +  "\t".join([q for q in qset])
    uset = set([url for url in words[2:]])
    # print "[DEBUG]url:\n" + "\t".join([u for u in uset])
    return words,qset,uset

def printQ(uid, qset, uset):
    print "\t".join([uid, "\t".join([q for q in qset]), "\t".join([u for u in uset])])

def main(argv):

    lastWords = []
    words = []

    querySet = set()
    urlSet = set()
    
    for line in sys.stdin:
        words = [d.strip() for d in line.strip().split("\t")]

        if len(lastWords) == 0:
            lastWords,querySet,urlSet = newQ(words)
            continue

        if lastWords[0] != words[0]:
            printQ(lastWords[0], querySet, urlSet)
            del querySet
            del urlSet
            lastWords,querySet,urlSet = newQ(words)
            continue
 
        querySet.add(words[1])
        for url in words[2:]:
            urlSet.add(url)       
        lastWords = words
 
    printQ(words[0], querySet, urlSet)
    del querySet
    del urlSet
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

