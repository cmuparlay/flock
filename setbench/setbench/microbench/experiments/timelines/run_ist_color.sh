#!/bin/sh

t=30000 step=10000 a=jemalloc r=debra p=none n=190 u=0.5 k=200000000 f=temp.txt
LD_PRELOAD=../../../lib/lib$a.so numactl --interleave=all ../../bin/ubench_brown_ext_ist_lf.alloc_new.reclaim_$r.pool_$p.out -nwork $n -nprefill $n -i $u -d $u -rq 0 -rqsize 1 -k $k -nrq 0 -t $t -pin `cd .. ; ./get_pinning_cluster.sh` > $f
cat $f | grep "total through"
cat $f | grep "global_epoch" | head -1
cat $f | grep "timeline_" | cut -d" " -f1 | sort | uniq -c

rm -f input_timeline_color.txt > /dev/null 2>&1
cat temp.txt | grep "timeline_helpRebuild" | cut -d" " -f2-4 | tr -d "=_a-zA-Z" | awk '{print $0 " black"}' >> input_timeline_color.txt
cat temp.txt | grep "timeline_freeSubtree" | cut -d" " -f2-4 | tr -d "=_a-zA-Z" | awk '{print $0 " red"}' >> input_timeline_color.txt
cat temp.txt | grep "timeline_rotateEpochBags" | cut -d" " -f2-4 | tr -d "=_a-zA-Z" | awk '{print $0 " blue"}' >> input_timeline_color.txt

python timeline_draw_color.py input_timeline_color.txt
