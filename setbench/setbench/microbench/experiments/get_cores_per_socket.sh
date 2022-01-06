#!/bin/sh
# 
# File:   get_cores_per_socket.sh
# Author: trbot
#
# Created on Jan 23, 2019, 6:42:32 PM
#
lscpu | grep "Core(s) per socket:" | cut -d":" -f2 | tr -d " "