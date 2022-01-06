#!/bin/bash
# 
# File:   leakchecks.sh
# Author: trbot
#
# Created on Aug 17, 2017, 6:26:03 PM
#

nwork=16
args="-i 10 -d 10 -k 1000 -rq 0 -rqsize 100 -t 1000 -nrq 0 -nwork $nwork"
valgrind_args="--fair-sched=yes --tool=memcheck --leak-check=yes --read-inline-info=yes --read-var-info=yes"
outdir=data_valgrind

if [ "$#" -eq "0" ]; then
    echo "USAGE: $0 binary_to_test [binary_to_test ...]"
    echo "Suggestion:"
    echo "$0 bin/[^_]*ubench*"
    exit
fi

valgrind_showerrors() {
    skiplines="HEAP SUMMARY|LEAK SUMMARY|definitely lost|still reachable|possibly lost|Reachable blocks|To see them|For counts of detected|to see where uninitialised values come|Memcheck|Copyright|Using Valgrind|Command:|ERROR SUMMARY|noted but unhandled ioctl|set address range perms|Thread"
    grep -E "== [^ ]" $1 | grep -vE "$skiplines"
    return 0
}

valgrind_showleaks() {
    skiplines="0 bytes"
    grep -E "definitely|possibly" $1 | grep -v "$skiplines"
    return 0
}

mkdir -p $outdir 2> /dev/null
curr=0
for bin in "$@" ; do
    ((curr = curr+1))

    fname=`echo "$bin.txt" | tr "/" "_"`
    fname=$outdir/$fname
    echo "step $curr / $# : $fname"
    valgrind $valgrind_args $bin $args > $fname 2>&1
    valgrind_showerrors $fname
    valgrind_showleaks $fname
done
