#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import sys

def main(argv):

    for line in sys.stdin:
        # uid timestamp date query url title refer_url
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 7:
            continue
        print "\t".join([words[3]] + words)

    return 0

if __name__ == "__main__":
    
    sys.exit(main(sys.argv))

