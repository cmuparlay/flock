#!/bin/sh
# 
# File:   get_cpu_ghz.sh
# Author: t35brown
#
# Created on 6-Aug-2019, 4:25:34 PM
#

#echo "scale=3 ;" `lscpu | grep "CPU max MHz" | cut -d":" -f2 | tr -d " "` "/ 1000" | bc
lscpu | grep "Model name" | cut -d"@" -f2 | tr -d " a-zA-Z"
