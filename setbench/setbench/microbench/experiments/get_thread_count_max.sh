#!/bin/bash
# 
# File:   get_thread_count_max.sh
# Author: trbot
#
# Created on Feb 20, 2019, 7:38 PM
#

sockets=`./get_numsockets.sh`
cores_per_socket=`./get_cores_per_socket.sh`
threads_per_core=`./get_threads_per_core.sh`
threads_per_socket=`expr $threads_per_core \* $cores_per_socket`

## reserve two threads for system use !!
echo `expr $threads_per_socket \* $sockets - 2`
