#!/bin/bash

cat data/summary.txt | head -1 | tr "," "\n" | tr -d " " | cut -d"=" -f1 | tr "\n" "," ; echo
cat data/summary.txt | grep "datastructure" | while read line ; do
    echo $line | tr "," "\n" | tr -d " " | cut -d"=" -f2- | tr "~" " " | tr "\n" "," ; echo
done
