#!/bin/bash

if [ "$#" -eq "0" ]; then
    echo "USAGE: $(basename $0) <INPUT_DATA_TXT_FILE> <LINE_SEARCH_STRING> [<LINE_SEARCH_STRING> ...]"
    echo "       (produces a horizontal strip of plots with filename out.png from provided search strings)"
    echo "   OR: $(basename $0) <INPUT_DATA_TXT_FILE>"
    echo "       (lists most sensible search strings in the file)"
    exit 1
fi

if [ "$#" -eq "1" ]; then
    fname=$1
    grep -E "^(log_hist[^=]*|lin_hist[^=]*|[^=]*_by_thread|[^=]*_by_index)=([0-9.:]+[ ]+)*[0-9.:]+$" $fname | cut -d"=" -f1 | sort | uniq
    exit 0
fi

fname=$1
SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
export PATH=$SCRIPTPATH/../../tools:$SCRIPTPATH:$PATH
outfile=out.png

shift

images=()
i=0
while [ "$#" -ge "1" ]; do
    searchstr=$1

    ftemp="_out_${i}.png"
    trial_to_plot.sh $fname $searchstr $ftemp
    images+=( $ftemp )

    shift
    i=$((i+1))
done

echo "\${images[@]}=${images[@]}"
convert "${images[@]}" +append $outfile
rm "${images[@]}"
