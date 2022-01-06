#!/bin/bash

file=abtree_kcas_scaling.csv

ins_loc=$(cat $file | head -n 1 | sed 's/\,INS_FRAC.*/\,,/' | sed -e 's/\(.\)/&\n/g' | grep "," | wc -l)
del_loc=$(cat $file | head -n 1 | sed 's/\,DEL_FRAC.*/\,,/' | sed -e 's/\(.\)/&\n/g' | grep "," | wc -l)
size_loc=$(cat $file | head -n 1 | sed 's/\,MAXKEY.*/\,,/' | sed -e 's/\(.\)/&\n/g' | grep "," | wc -l)
threads_loc=$(cat $file | head -n 1 | sed 's/\,TOTAL_THREADS.*/\,,/' | sed -e 's/\(.\)/&\n/g' | grep "," | wc -l)
targ_loc=$(cat $file | head -n 1 | sed "s/\\,$1.*/\\,,/" | sed -e 's/\(.\)/&\n/g' | grep "," | wc -l)

dses=$(cat $file | cut -d "," -f2 | sort | uniq -d | tr "\n" " " |  sed 's/$ //')

truncate gnu.txt --size 0

echo DS_TYPENAME $(cat $file | cut -d "," -f"$threads_loc" | sort -g | uniq -d  | tr "\n" " ") >> gnu.txt

for ds in $dses; do
    echo $ds $(cat $file | cut -d "," -f2,"$ins_loc,$del_loc,$size_loc,$threads_loc,$targ_loc" | grep $ds",.*"$2","$3","$4"," | cut -d "," -f6 | tr "\n" " " ) >> gnu.txt
done

gnuplot -p -e "\
            set terminal pngcairo notransparent noenhanced font \"arial,10\" fontscale 1.0 size 1920, 1080; \
            set output \"graph.png\"; set ylabel \""$1"\"; \
            set title \""$1" scaling with "$2"% inserts, "$3"% deletes and keyrange size of "$4"\"; \
            set key under; \
            set key autotitle columnheader; \
            set style fill solid 1.00 border lt -1; \
            set boxwidth 1 absolute; \
            set datafile separator ' '; \
            set auto y; \
            set yrange [0:];\
            set style data histogram; \
            set style histogram cluster gap 1; \
            set grid; plot 'gnu.txt' using 2, \
                '' using 3, \
                '' using 4, \
                '' using 5, \
                '' using 6:xticlabels(1), \
                '' using (\$0 - 2/6.0):2:2 with labels right offset 0,1 notitle, \
                '' using (\$0 - 1/6.0):3:3 with labels right offset 0,1 notitle, \
                '' using (\$0):4:4 with labels right offset 0,1 notitle, \
                '' using (\$0 + 1/6.0):5:5 with labels right offset 0,1 notitle, \
                '' using (\$0 + 2/6.0):6:6 with labels right offset 0,1 notitle"
feh graph.png