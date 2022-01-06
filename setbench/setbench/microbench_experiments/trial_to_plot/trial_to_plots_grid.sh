#!/bin/bash

if [ "$#" -eq "0" ]; then
    echo "USAGE: $(basename $0) <INPUT_DATA_TXT_FILE> <NUM_IMAGE_COLUMNS> <LINE_SEARCH_STRING> [<LINE_SEARCH_STRING> ...]"
    echo "       (produces a grid of plots with filename out_grid.png from the provided search strings)"
    echo "   OR: $(basename $0) <INPUT_DATA_TXT_FILE> <NUM_IMAGE_COLUMNS>"
    echo "       (produces a grid of plots with filename out_grid.png from ALL sensible search strings)"
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
ncols=$2

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
export PATH=$SCRIPTPATH/../../tools:$SCRIPTPATH:$PATH

if [ "$#" -eq "2" ]; then
    echo "## recursive shell invocation to easily obtain args..."
    eval $(basename $0) $fname $ncols $($(basename $0) $fname)
    exit 0
fi

outfile=out_grid.png

echo
echo "## trial_to_plots_grid.sh $@"
echo

shift
shift

try_process_row() {
    if [ "${#row_searchstrings[@]}" -ne "0" ]; then
        echo "## trial_to_plots_strip.sh $fname" "${row_searchstrings[@]}"
        trial_to_plots_strip.sh $fname "${row_searchstrings[@]}" ####### produces "out.png"
        mv out.png "_out_row_${r}.png"
        row_images+=( "_out_row_${r}.png" )
        row_searchstrings=()
        r=$((r+1))
    fi
}

i=0
r=0
row_searchstrings=()
row_images=()
while [ "$#" -ge "1" ]; do
    searchstr=$1
    row_searchstrings+=( "$searchstr" )
    shift
    i=$((i+1))
    if (( (i % ncols == 0) )); then
        try_process_row
    fi
done
try_process_row

# echo "\${row_images[@]}=${row_images[@]}"
convert -background "#000000" "${row_images[@]}" -append $outfile
rm "${row_images[@]}"
