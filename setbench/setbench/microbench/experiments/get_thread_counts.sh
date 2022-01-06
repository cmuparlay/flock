#!/bin/bash
# 
# File:   get_thread_counts.sh
# Author: trbot
#
# Created on Jan 23, 2019, 6:43:25 PM
#

sockets=`./get_numsockets.sh`
cores_per_socket=`./get_cores_per_socket.sh`
threads_per_core=`./get_threads_per_core.sh`
threads_per_socket=`expr $threads_per_core \* $cores_per_socket`
#thread_counts="1"
if (($cores_per_socket < $threads_per_socket)) ; then
    thread_counts="$thread_counts $cores_per_socket"
fi
for ((i=1;i<$sockets;++i)) ; do
    threads_per_i_sockets=`expr $threads_per_socket \* $i`
    thread_counts="$thread_counts $threads_per_i_sockets"
done
threads_per_i_sockets=`expr $threads_per_socket \* $sockets - 2` ## reserve 2 threads for system use
thread_counts="$thread_counts $threads_per_i_sockets"
echo $thread_counts
