#!/bin/bash
# 
# File:   runscript.sh
# Author: trbot
#
# Created on Jul 18, 2017, 10:56:28 PM
#

## Thread counts to run in the experiments.
thread_counts="1 18 36 72 108 144"

## Configure the thread pinning/binding policy.
## Blank means no thread pinning. (Threads can run wherever they want.)
pinning_policy="-pin 0-17.72-89.18-35.90-107.36-53.108-125.54-71.126-143"

ntrials=5
warehouses=36

outpath=data
fsummary=$outpath/summary.txt

rm -r -f $outpath.old > /dev/null 2>&1
mv $outpath $outpath.old > /dev/null 2>&1
mkdir -p $outpath

cnt1=10000
cnt2=10000

## fix any \r line endings, which mess up the DBMS schema in TPC-C
dos2unix benchmarks/*.txt

for counting in 1 0 ; do
    if (($counting==0)); then
        echo "Total trials: $cnt1 ... $cnt2"
    fi
    
    for nthreads in $thread_counts ; do
    for ((trial=0; trial < $ntrials; ++trial)); do
    for exepath in `ls ./bin/rundb_TPCC_*`; do
        if (($counting==1)); then
            cnt2=`expr $cnt2 + 1`
            if ((($cnt2 % 100) == 0)); then echo "Counting trials: $cnt2" ; fi
        else
            cnt1=`expr $cnt1 + 1`
            exeonly=`echo $exepath | cut -d"/" -f3`
            fname=$outpath/step$cnt1.txt
            workload=`echo $exeonly | cut -d"_" -f2`
            datastructure=`echo $exeonly | cut -d"_" -f3- | cut -d"." -f1`

            args="-t$nthreads -n$warehouses ${pinning_policy}"
            cmd="env LD_PRELOAD=${jemalloc_path} $exepath $args"
            nospaces=`echo "$cmd" | tr " " "~"`
            echo -n "step=$cnt1, trial=$trial, workload=$workload, datastructure=$datastructure, warehouses=$warehouses, cmd=$nospaces, fname=$fname," >> $fsummary
            echo $fname > $fname
            echo $cmd >> $fname
            $cmd >> $fname
            cat $fname | grep "summary" | cut -d"]" -f2- >> $fsummary
            tail -1 $fsummary
        fi
    done
    done
    done
done

./makecsv.sh
