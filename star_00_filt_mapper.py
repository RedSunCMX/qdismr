#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@360.cn
# [Date]:   2013/07/26

import sys

def printLog(words, str):
    # nquery "A"|"B" uid timestamp nurl title refer_url
    print "\t".join([words[0], str, words[8], words[4], words[1], words[7], words[6]])

def main(argv):

    for line in sys.stdin:
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 11:
            continue
        printLog(words, "A")
        printLog(words, "B")

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

