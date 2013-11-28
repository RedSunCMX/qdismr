#!/bin/bash

pfix=star_01_url_cls

#ls -al ./epd.tar.gz/epd-7.1-2-rh3-x86_64/lib/ >&2
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:`pwd`/epd.tar.gz/epd-7.1-2-rh3-x86_64/lib/
#echo $LD_LIBRARY_PATH >&2
epd.tar.gz/epd-7.1-2-rh3-x86_64/bin/python star_01_url_cls_mapper.py

