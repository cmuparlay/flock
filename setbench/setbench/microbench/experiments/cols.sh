#!/bin/bash

if [ "$#" -le "1" ]; then
	echo "USAGE: ./cols.sh FILENAME COLUMN_SEARCH_STRING [[COLUMN_SEARCH_STRING] ...]"
	exit 1
fi

fname="$1"
shift

printf "%20s " "$fname"
for ((i=$#;i>0;--i)) ; do
	f="$1"
	shift
	searchstr="${f}.*="
#	printf "searchstr=$searchstr\n"
	nlines=`cat $fname | grep "$searchstr" | wc -l | tr -d " "`
#	if [ "$nlines" -gt "1" ]; then
#		echo "error: $nlines lines returned for grep query [$searchstr]..."
#		cat $fname | grep "$searchstr"
#		exit 1
#	fi
	val=`cat $fname | grep "$searchstr" | tail -1 | cut -d"=" -f2 | tr -d " "`
	if [ "$nlines" -gt "1" ]; then
		val="$val ($nlines)"
	fi
	printf "%20s " "$val"
done

