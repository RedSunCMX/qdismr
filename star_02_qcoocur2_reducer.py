#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import sys

#  bdict: key: cid value: url_set
#  words: query "B" cnt uid \1 date url_list
def printQB(query, words, bdict):
    if len(bdict) == 0:
        print "\t".join([words[3], "q" + query + "\2" + "-" + "\1" + words[2], \
          "\t".join(["u" + url for url in words[4:]])])
        return 

    url_set_a = set([url.split("\1")[0] for url in words[4:]])
     
    qcid = "-"
    max_join = -1
    if len(url_set_a) > 0:
        for cid in bdict:
            cnt = len(url_set_a & bdict[cid])
            if cnt > 0 and max_join < cnt:
                max_join = cid
                qcid = cid

    if qcid is "-":
        for cid in bdict:
            cnt = len(bdict[cid])
            if max_join < cnt:
                max_join = cnt
                qcid = cid
    
    print "\t".join([words[3], "q" + query + "\2" + qcid + "\1" + words[2], \
      "\t".join(["u" + url for url in words[4:]])])

def newQB(words):
    adict = {}
    if words[1] == "B": 
        printQB(words[0], words, adict)
    elif words[1] == "A":
        if words[2] not in adict:
            adict[words[2]] = set()
        adict[words[2]].add(words[3])
    return words,adict

def main(argv):

    lastWords = []
    words = []
    ADict = {}
    # query "B" cnt uid \1 date url_list
    # query "A" CID url
    for line in sys.stdin:
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) < 3:
            continue

        if words[0] is "":
            continue

        # print "\t".join(words)
        if len(lastWords) == 0:
            lastWords,ADict = newQB(words)
            continue

        if lastWords[0] != words[0]:
            del ADict
            lastWords,ADict = newQB(words)
            continue
    
        if words[1] == "A":
            if words[2] not in ADict:
                ADict[words[2]] = set()
            ADict[words[2]].add(words[3])

        if words[1] == "B":
            printQB(words[0], words, ADict)

        lastWords = words
    
    del ADict
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

