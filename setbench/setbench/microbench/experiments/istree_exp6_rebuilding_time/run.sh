#!/bin/bash

#########################################################################
#### Experiment configuration
#########################################################################

## approx sec to prefill 1B keys is (5+17)/2 = 11s [interpolating 192 and 24 threads]
## approx sec to prefill 20M keys is likely ~2s
## so avg prefill cost is approx (3*2s + 11s) / 4 = 4.3s 
## so estimate ~35s total for one execution
## so estimate 35 min per trial -- confirmed at 35.33m
## (with both enabled and disabled trials, new trial estimate of 71m!! total 3.5h)

## with 120s x 3 trials and one update rate, now estimated at 125m...

num_trials=3
halved_update_rates="0.5 5 20"
durations_ms="60000"
key_range_sizes="2000000 20000000 200000000 2000000000"
algorithms="brown_ext_ist_lf"
thread_counts=`cd .. ; ./get_thread_counts.sh`

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
        for mode in disabled enabled ; do
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
                                    cmd="LD_PRELOAD=../../../lib/libjemalloc.so timeout $timeout_s numactl --interleave=all time ./bin_${mode}/ubench_${alg}.alloc_new.reclaim_debra.pool_none.out $args"
                                    echo "cmd=$cmd" > $f
                                    echo "step=$step" >> $f
                                    echo "fname=$f" >> $f

#                                    echo "eval $cmd >> $f 2>&1"
                                    eval $cmd >> $f 2>&1
                                    if [ "$?" -ne "0" ]; then
                                        cat $f
                                    fi

                                    ## manually parse the maximum resident size from the output of `time` and add it to the step file
                                    maxres=`../grep_maxres.sh $f 2> /dev/null`
                                    echo "maxresident_mb=$maxres" >> $f

                                    ## parse step file to extract fields of interest, and also *rename* the algorithm to explicitly encode whether the functionality is enabled or disabled
                                    ../parse.sh $f | tail -1 | sed "s/${alg}/${alg}_${mode}/g" >> $exp.csv
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
done

echo "started: $started" | tee "time_started.txt"
echo "finished:" `date` | tee "time_finished.txt"

zip -r ${exp}.zip ${exp} ${exp}.csv *.sh
rm -f data.csv 2> /dev/null # clean up after parse.sh
