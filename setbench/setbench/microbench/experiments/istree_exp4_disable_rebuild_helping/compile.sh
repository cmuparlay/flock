#!/bin/sh

compile_args_for_disabled="-DIST_DISABLE_REBUILD_HELPING -DMEASURE_REBUILDING_TIME"
compile_args_for_enabled="-DMEASURE_REBUILDING_TIME"

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