#!/bin/sh
# 
# File:   get_numsockets.sh
# Author: trbot
#
# Created on Jan 23, 2019, 6:42:00 PM
#

lscpu | grep "Socket(s):" | cut -d":" -f2 | tr -d " "