#!/bin/bash

#########################################################################
#### Experiment configuration
#########################################################################

## estimate prefilling takes max 4s for 200M, and probably ~2s for smaller, let's say 3s avg
## so estimate total time of sum(duration_ms) + 3*count(duration_ms) = 148s over all time durations
## so estimate 2h per trial (so 4h total) -- confirmed at 1h56m

num_trials=2
halved_update_rates="20 5 0.5 0"
durations_ms="1000 2000 4000 8000 16000 32000 64000" ## total 127s, plus lets say 5s per run, so 162s total
key_range_sizes="2000000 20000000 200000000"
algorithms="brown_ext_ist_lf brown_ext_abtree_lf bronson_pext_bst_occ natarajan_ext_bst_lf"
thread_counts=`cd .. ; ./get_thread_count_max.sh`

#########################################################################
#### Produce header
#########################################################################

timeout_s=600
exp="`pwd | rev | cut -d'/' -f1 | rev`"

mkdir $exp 2>/dev/null
../parse.sh null > $exp.csv
cat $exp.csv

step=10000
maxstep=$step
pinning_policy=`cd .. ; ./get_pinning_cluster.sh`

#########################################################################
#### Run trials
#########################################################################

started=`date`
for counting in 1 0 ; do
    for ((trial=0;trial<num_trials;++trial)) ; do
        for uhalf in $halved_update_rates ; do
            for k in $key_range_sizes ; do
                for alg in $algorithms ; do
                    for n in $thread_counts ; do
                        for t in $durations_ms ; do
                            if ((counting)); then
                                maxstep=$((maxstep+1))
                            else
                                step=$((step+1))
                                if [ "$#" -eq "1" ]; then ## check if user wants to just replay one precise trial
                                    if [ "$1" -ne "$step" ]; then
                                        continue
                                    fi
                                fi

                                f="$exp/step$step.txt"
                                args="-nwork $n -nprefill $n -i $uhalf -d $uhalf -rq 0 -rqsize 1 -k $k -nrq 0 -t $t -pin $pinning_policy"
                                cmd="LD_PRELOAD=../../../lib/libjemalloc.so timeout $timeout_s numactl --interleave=all time ../../bin/ubench_${alg}.alloc_new.reclaim_debra.pool_none.out $args"
                                echo "cmd=$cmd" > $f
                                echo "step=$step" >> $f
                                echo "fname=$f" >> $f

#                                echo "eval $cmd >> $f 2>&1"
                                eval $cmd >> $f 2>&1
                                if [ "$?" -ne "0" ]; then
                                    cat $f
                                fi

                                ## manually parse the maximum resident size from the output of `time` and add it to the step file
                                maxres=`../grep_maxres.sh $f 2> /dev/null`
                                echo "maxresident_mb=$maxres" >> $f

                                ## parse step file to extract fields of interest
                                ../parse.sh $f | tail -1 >> $exp.csv
                                echo -n "step $step/$maxstep: "
                                cat $exp.csv | tail -1
                            fi
                        done
                    done
                done
             done
        done
    done
done

echo "started: $started" | tee "time_started.txt"
echo "finished:" `date` | tee "time_finished.txt"

zip -r ${exp}.zip ${exp} ${exp}.csv *.sh
rm -f data.csv 2> /dev/null # clean up after parse.sh
