#!/bin/bash

rm -r -f temp
mkdir temp

echo "Non-zero failed RQ counts:" > failed_rq_counts.out

printf "%10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n" "rep" "alg" "workthr" "rqthr" "ins" "del" "k" "updates" "total_rq" "succ_rq" "fail_rq" "paths" > mass_trials.out
#echo "rep,algorithm,workthr,rqthr,updates,total_rq,succ_rq,fail_rq,paths" > mass_trials.out
cat mass_trials.out | tr "," "\t"
for ((trial=1000;trial<1200;++trial)); do
	(( k=$RANDOM % 9999 + 2 ))
	(( i=$RANDOM % 100 ))
	(( d=100 - $i ))
	(( nwork=$RANDOM % 48 + 1 ))
	(( nrq=$RANDOM % 48 + 1 ))
	#(( rqsize=$RANDOM % $k + 1 ))
	rqsize=$k

#	echo k=$k i=$i d=$d nwork=$nwork nrq=$nrq rqsize=$rqsize

#	for nwork_nrq in 47_1 ; do
#	for nwork_nrq in 40_8 ; do
#	for nwork_nrq in 24_24 ; do
#		nwork=`echo $nwork_nrq | cut -d"_" -f1`
#		nrq=`echo $nwork_nrq | cut -d"_" -f2`
		for ds in lflist ; do
		for alg in lockfree rwlock ; do # rwlock unsafe unsafe_ts ; do
			fname="temp/trial$trial.nwork${nwork}.nrq${nrq}.$alg.out"
			LD_PRELOAD="./lib/libjemalloc-5.0.1-25.so" ./`hostname`.${ds}.rq_$alg.out -t 1000 -i $i -d $d -rq 0 -rqsize $rqsize -k $k -p -ma new -mr debra -mp none -nrq $nrq -nwork $nwork > $fname
			updates=`grep "throughput" $fname | cut -d":" -f2 | tr -d " "`
			failrq=`grep "RQ VALIDATION TOTAL FAILURES" $fname | cut -d":" -f2 | tr -d " "`
			succrq=`grep "RQ VALIDATION TOTAL SUCCESSES" $fname | cut -d":" -f2 | tr -d " "`
			totalrq=`grep "rq succ" $fname | cut -d":" -f2 | tr -d " "`
			#succrq=`expr $totalrq - 0$failrq`
                        paths=`grep "code path exec" $fname | cut -d":" -f2 | cut -d" " -f2-`
			printf "%10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s" "$trial" "$alg" "$nwork" "$nrq" "$i" "$d" "$k" "$updates" "$totalrq" "$succrq" "$failrq" >> mass_trials.out
			grep "DEBUG" $fname
			for x in $paths ; do printf "%10s " "$x" >> mass_trials.out ; done
			printf "\n" >> mass_trials.out
			tail -1 mass_trials.out

			if [ "0$failrq" -ne "0" ]; then echo "Trial $trial: failed $failrq RQs" >> failed_rq_counts.out ; fi
		done
		done
#	done
done

grep "RQ VALIDATION ERROR" temp/* > failedrqs.out
cat failedrqs.out

rm failedrqs.out
