#!/bin/bash

files+=( $(find ../ -name "__pycache__") )
files+=( $(find ../ -name "perf.data*") )
files+=( $(find ../ -name "*.dot") )
files+=( $(find ../ -name "log*.txt") )
files+=( $(find ../ -name "step*.txt") )
files+=( $(find ../ -name "out*.svg") )
files+=( $(find ../ -name "out*.png") )
files+=( $(find ../ -name "temp_*.txt") )
files+=( $(find ../ -name "temp.*.txt") )
files+=( $(find ../ -name "temp.txt") )
files+=( $(find ../ -name "time*.txt") )
files+=( $(find ../ -name "*_bin") )
files+=( $(find ../ -name "*.old") )
files+=( $(find ../ -name "*.zip") )
files+=( $(find ../ -name "*.png") )
files+=( $(find ../ -name "*.gif") )
files+=( $(find ../ -name "*.mp4") )
files+=( $(find ../ -name "*.htm") )
files+=( $(find ../ -name "data*.txt") )
files+=( $(find ../ -name "_*.txt") )
files+=( $(find ../ -name "*.csv") )
files+=( $(find ../ -name "*.sqlite") )
files+=( $(find ../ -name "plot*.txt") )
files+=( $(find ../ -name "out*.txt") )
files+=( $(find ../ -name "log.*") )
files+=( $(find ../ -name "bin") )
files+=( $(find ../ -name "data") )
files+=( $(find ../ -name "_output_backup") )
files+=( $(find ../ -name "_private") )


if [ "${#files[@]}" -ne "0" ]; then
    echo "files:"
    echo "${files[@]}" | tr " " "\n"
    rm -r ${files[@]}
fi

## stop and remove any existing docker container named setbench
docker stop setbench 2>/dev/null
echo y | docker container rm setbench 2>/dev/null
echo 'finished CI clean'
