#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@360.cn
# [Date]:   2013/07/26

import sys

def printLog(words, str):
    # nquery "A"|"B" uid date nurl title
    for w in words[4:]:
        print "\t".join([words[0], str, w, words[3], words[1], words[2]])

def main(argv):

    for line in sys.stdin:
        words = [d.strip() for d in line.strip().split("\t")]
        # if len(words) != 11:
        #    continue
        printLog(words, "A")
        printLog(words, "B")

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

