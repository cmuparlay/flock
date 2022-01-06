#!/bin/bash

exp="data"
rm $exp.csv 2>/dev/null
mkdir $exp 2>/dev/null
step=10000
for ((trial=0;trial<10;++trial)) ; do
for u in 0 0.1 10 ; do
	for n in 1 3 6 9 12 15 18 21 24 27 30 33 36 ; do
		for alg in brown_ext_bst_glock brown_ext_bst_rwlock brown_ext_bst_hohlock brown_ext_bst_hohrwlock guerraoui_ext_bst_ticket ; do
			step=$(( step + 1 ))
			args="-nwork $n -nprefill 1 -i $u -d $u -rq 0 -rqsize 100 -k 200000 -nrq 0 -t 10000 -pin 0-17,72-89,18-35,90-107,36-53,108-125,54-71,126-143"
			LD_PRELOAD=../../../lib/libjemalloc-5.0.1-25.so numactl --i 0 ../../bin/${alg}.ubench_rnone $args > $exp/step$step.txt
			if [ "$step" -eq "10001" ] ; then
				../parse.sh $exp/step$step.txt > $exp.csv
				cat $exp.csv
			else
				../parse.sh $exp/step$step.txt | tail -1 >> $exp.csv
				cat $exp.csv | tail -1
			fi
		done
	done
done
done
