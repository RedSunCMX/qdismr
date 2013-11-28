# =*- coding:utf-8 -*-
## !/usr/bin/env python
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import sys
import star_01_url_cls_utils

def main(argv):
    K = 3
    if len(argv) == 2:
        K = int(argv[1])
    sc = star_01_url_cls_utils.spectralClusU()

    def newQ(ws):
        logs = []
        logs.append(ws)
        return ws,logs 

    lastWords = []
    words = []
    logsInQ = []
    query_id,uid_id,date_id,url_id,title_id,refer_id = \
      0,3,4,5,6,7
    for line in sys.stdin:
        # nquery uid date nurl title refer_url
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 8:
            continue

        if words[0] == "" or words[1] == "" or words[2] == "" or \
          words[3] == "" or words[4] == "" or words[5] == "" or \
          words[6] == "" or words[7] == "":
            continue
        # print "\t".join(words)
        if len(lastWords) == 0:
            lastWords,logsInQ = newQ(words)
            continue
        if words[0] != lastWords[0]:
            try:
                sc.sc_exec(logsInQ, K)
            except ValueError, e:
                pass
            del logsInQ
            lastWords,logsInQ = newQ(words)
            continue
        logsInQ.append(words)
        lastWords = words

    if len(logsInQ) > 0:
        try:
            sc.sc_exec(logsInQ, K)
        except ValueError, e:
            pass
        del logsInQ
    return 0
 
if __name__ == "__main__":
    sys.exit(main(sys.argv))

