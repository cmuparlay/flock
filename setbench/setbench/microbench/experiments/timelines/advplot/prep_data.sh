#!/bin/bash

if [ "$#" -lt "2" ]; then
    echo "USAGE: ./prep_data.sh INPUT_FILE OUTPUT_FILE"
    exit 1
fi

ftempa=temp_a.txt
ftempb=temp_b.txt
fin=$1
fout=$2

echo
echo "Found the following event types and counts"
cat $fin | grep "timeline_" | cut -d" " -f1 | sort | uniq -c
echo

cat $fin | grep "timeline_" | cut -d" " -f1 | sed 's/timeline_//' > $ftempa
cat $fin | grep "timeline_" | cut -d" " -f2- | tr -d "=_a-zA-Z" > $ftempb
paste -d " " $ftempa $ftempb > $fout
echo "produced file $fout"
echo

rm $ftempa
rm $ftempb
