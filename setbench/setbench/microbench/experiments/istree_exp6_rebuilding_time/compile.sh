#!/bin/sh

args_for_both="-DMEASURE_DURATION_STATS"
compile_args_for_disabled="-DIST_DISABLE_REBUILD_HELPING ${args_for_both}"
compile_args_for_enabled="${args_for_both}"

echo "#########################################################################"
echo "#### Compiling binaries with the desired functionality disabled"
echo "#########################################################################"

here=`pwd`
mkdir ${here}/bin 2>/dev/null
cd .. ; cd .. ; make -j all bin_dir=${here}/bin_disabled xargs="$compile_args_for_disabled" > compiling.txt 2>&1
if [ "$?" -ne "0" ]; then
    echo "ERROR compiling; see compiling.txt"
    exit
fi
cd $here

echo "#########################################################################"
echo "#### Compiling binaries with the desired functionality enabled"
echo "#########################################################################"

here=`pwd`
mkdir ${here}/bin 2>/dev/null
cd .. ; cd .. ; make -j all bin_dir=${here}/bin_enabled xargs="$compile_args_for_enabled" >> compiling.txt 2>&1
if [ "$?" -ne "0" ]; then
    echo "ERROR compiling; see compiling.txt"
    exit
fi
cd $here
