#!/usr/bin/env python
# =*- coding:utf-8 -*-
#
# [Author]: Chen Mingxing, chenmingxing@baidu.com
# [Date]:   2013/07/26

import time, sys
import base64

def newQ(words):
    udict = {}
    udict[words[4]] = 1
    return words,1,words[1],udict

def printQ(words, uv, udict):
    urlTOP = 500
    tdict = {}
    if uv >= 100:
        if len(udict) > urlTOP:
            # no = 0
            for url,cnt in sorted(udict.items(), \
              key=lambda e:e[1], reverse=True)[:urlTOP]:
                tdict[url] = cnt
                # no += 1
                # if no >= 10000:
                #    break
            del udict
        else:
            tdict = udict

        if words[4] in tdict:
            # title_str = words[5]
            title_str = base64.decodestring(words[5])
            if title_str.strip() == "":
                title_str = "-"

            date_str = time.strftime("%Y%m%d", \
              time.localtime(float(words[3])))

            # nquery uid date nurl title refer_url
            print "\t".join([words[0], "UV:%d"%(uv), \
              "url:%d"%(tdict[words[4]]), words[2], \
              date_str, words[4], title_str, words[6]])

    return tdict

def main(argv):

    lastWords = []
    words = []
    uv = 1
    str = "A"
    url_dict = {}
    # nquery "A"|"B" uid timestamp nurl title refer_url
    for line in sys.stdin:
        words = [d.strip() for d in line.strip().split("\t")]
        if len(words) != 7:
            continue

        if len(lastWords) == 0:
            lastWords,uv,str,url_dict = newQ(words)
            continue

        if lastWords[0] != words[0]:
            # printQ(words, uv)
            lastWords,uv,str,url_dict = newQ(words)
            continue

        if words[1] == str:
            if lastWords[2] != words[2]:
                uv += 1
            if words[4] not in url_dict:
                url_dict[words[4]] = 1
            else:
                url_dict[words[4]] += 1
            lastWords = words
            continue

        # words[1] != str 
        url_dict = printQ(words, uv, url_dict)
        lastWords = words 

if __name__ == "__main__":
    sys.exit(main(sys.argv))

