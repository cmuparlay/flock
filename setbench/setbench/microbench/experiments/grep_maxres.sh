#!/bin/sh

f=$1
maxres=`cat $f | grep "maxres" | cut -d" " -f6 | cut -d"m" -f1`
#echo "maxres=$maxres"
maxres=`echo "$maxres / 1000" | bc`
#printf "%20s" "${maxres}mb"
echo -n $maxres