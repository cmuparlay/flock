#!/bin/bash

if [ "$#" -lt "5" ]; then
    echo "USAGE: ./do_plot_bw.sh INPUT_FILE OUTPUT_FILE EVENT_SEARCH_STRING COLOR [[EVENT_SEARCH_STRING COLOR] ...]"
    exit 1
fi

ftempa=temp_a.txt
ftempb=temp_b.txt
ftempc=temp_c.txt
fin=$1
fout=$2

shift
shift

echo
echo "Found the following event types and counts"
cat $fin | grep "timeline_" | cut -d" " -f1 | sort | uniq -c
echo

cat $fin | grep "timeline_" | cut -d" " -f1 | sed 's/timeline_//' > $ftempa
cat $fin | grep "timeline_" | cut -d" " -f2- | tr -d "=_a-zA-Z" > $ftempb
paste -d " " $ftempa $ftempb > $ftempc

python2 ./timeline_advplot.py $ftempc $fout $@
if [ "$?" -eq "0" ]; then
    echo
    if [ "$fout" != "__show__" ]; then
        echo "Should have produced image: $fout"
    fi
fi

rm $ftempa
rm $ftempb
