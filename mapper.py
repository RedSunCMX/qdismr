#!/usr/bin/python
# -*- coding:utf8 -*-

# hadoop counter sample code

import sys

# tdict=("无毒不丈夫" "真心话大冒险" "火箭" "魔烟" "夜空" "ef" "df")
def main(argv):
    # str_reg = re.compile("ef", re.IGNORECASE)
    # str_reg = re.compile("魔烟", re.IGNORECASE)
    line = sys.stdin.readline()
    while line:
        line = line.strip()
    # print line
        arr = line.split("\t")
        if len(arr) == 7:
            # isIn = str_reg.match(arr[3])
            # if isIn:
            # if arr[3].find("魔烟") >= 0:
            # if arr[3].find("真心话大冒险") >= 0:
            # if arr[3].find("无毒不丈夫") >= 0:
            # if arr[3].find("火箭") >= 0:
            # if arr[3].find("夜空") >= 0:
            # if arr[3].find("df") >= 0 or arr[3].find("dF") >= 0 or arr[3].find("Df") >= 0 or arr[3].find("DF") >= 0:
            # if arr[3].find("三中全会") >= 0:
            # if arr[3].find("消失的荒野") >= 0:
            # if arr[3].find("ef") >= 0 or arr[3].find("eF") >= 0 or arr[3].find("Ef") >= 0 or arr[3].find("EF") >= 0:
            # if arr[3].find("张震") >= 0:
            # if arr[3].find("巴赫") >= 0:
            # if arr[3].find("格莱美") >= 0:
            if arr[3].find("乌托邦") >= 0:
                print line
        line = sys.stdin.readline()
    return

if __name__ == "__main__":
    main(sys.argv)


