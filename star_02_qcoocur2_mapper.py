#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@360.cn
# [Date]:   2013/07/26

import sys
import os

def printInPath1(words):
    qdict = {}
    udict = {}
    # uid\0001date
    for w in words[1:]:
        if w[0] == "q":
            query = w[1:]
            if query not in qdict:
                qdict[query] = 1
        elif w[0] == "u":
            url = w[1:]
            if url not in udict:
                udict[url] = 1

    if len(qdict) > 10000:
        return
    if len(udict) > 10000:
        return

    url_list = "\t".join([url.strip() for url in udict])
    for key in qdict:
        # try:
        qs = key.split("\1")
        qs[0] = qs[0].strip()
        if qs[0] is "":
            continue
        print "\t".join([qs[0], "B", qs[1], words[0], url_list])
        # except IOError, e:
        #    continue 

def printInPath2(words):
    if len(words) < 3:
        return
    if words[0] is "":
        return
    # query \t CID \t url
    print "\t".join([words[0], "A", "\t".join(words[1:])]) 

def main(argv):
    input_path1 = "hdfs://w-namenode.qss.zzbc2.qihoo.net:9000/home/nlp/gongyk/query_correlation/query_coocur2"
    input_path2 = "hdfs://w-namenode.qss.zzbc2.qihoo.net:9000/home/nlp/mingxing/mpi/star_01_url_cls_qnorm/output/output/rank-*"

    for line in sys.stdin:
        words = [d.strip() for d in line.strip().split("\t")]
        # "hdfs://w-namenode.qss.zzbc2.qihoo.net:9000/home/nlp/mingxing/test/input/path1/part-00000"
        dir_name = os.path.dirname(os.environ['map_input_file'])
        if dir_name == input_path1:
            printInPath1(words)
        else:
            printInPath2(words)
    
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

