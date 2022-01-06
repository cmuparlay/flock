#!/bin/sh
# 
# File:   get_threads_per_core.sh
# Author: trbot
#
# Created on Jan 23, 2019, 6:43:05 PM
#
lscpu | grep "Thread(s) per core:" | cut -d":" -f2 | tr -d " "