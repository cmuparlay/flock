#!/bin/bash

#########################################################################
#### Experiment configuration
#########################################################################

## estimated ? per trial

t=30000
num_trials=1
halved_update_rates="0.5 20"
key_range_sizes="20000000"
algorithms="ellen_ext_bst_lf guerraoui_ext_bst_ticket howley_int_bst_lf ramachandran_int_bst_lf brown_ext_bst_rq_lf drachsler_pext_bst_lock brown_ext_ist_lf brown_ext_abtree_lf bronson_pext_bst_occ natarajan_ext_bst_lf"
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

## if user provides any argument, then we are running in TESTING mode, with 1ms runs and no prefilling
if [ "$1" != "" ]; then
    echo "*** WARNING *** running in TESTING mode (1ms runs; no prefill; max threads only)"
    thread_counts=`cd .. ; ./get_thread_count_max.sh`
fi

started=`date`
for counting in 1 0 ; do
    for ((trial=0;trial<num_trials;++trial)) ; do
        for uhalf in $halved_update_rates ; do
            for k in $key_range_sizes ; do
                for alg in $algorithms ; do
                    for n in $thread_counts ; do
                        if ((counting)); then
                            maxstep=$((maxstep+1))
                        else
                            step=$((step+1))
#                            if [ "$#" -eq "1" ]; then ## check if user wants to just replay one precise trial
#                                if [ "$1" -ne "$step" ]; then
#                                    continue
#                                fi
#                            fi

                            ## if user provides any argument, then we are running in TESTING mode, with 1ms runs and no prefilling
                            if [ "$1" != "" ]; then
                                addon="-nprefill 0 -t 1"
                            else
                                addon="-nprefill $n -t $t"
                            fi
                            args="$addon -nwork $n -i $uhalf -d $uhalf -rq 0 -rqsize 1 -k $k -nrq 0 -pin $pinning_policy"

                            f="$exp/step$step.txt"
                            cmd="LD_PRELOAD=../../../lib/libjemalloc.so timeout $timeout_s numactl --interleave=all time ../../bin/ubench_${alg}.alloc_new.reclaim_debra.pool_none.out $args"
                            echo "cmd=$cmd" > $f
                            echo "step=$step" >> $f
                            echo "fname=$f" >> $f

#                            echo "eval $cmd >> $f 2>&1"
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

echo "started: $started" | tee "time_started.txt"
echo "finished:" `date` | tee "time_finished.txt"

zip -r ${exp}.zip ${exp} ${exp}.csv *.sh
rm -f data.csv 2> /dev/null # clean up after parse.sh
