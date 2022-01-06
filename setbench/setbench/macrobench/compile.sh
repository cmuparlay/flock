#!/bin/bash
#
# File:   compile.sh
# Author: trbot
#
# Created on May 28, 2017, 9:56:43 PM
#

workloads="YCSB"

## format is the following is
## data_structure_name:compilation_arguments
algs=( \
    # "aksenov_splaylist_64:" \ DO NOT USE WITH TOO HIGH KEY COUNT (DOES NOT RECLAIM MEM)
    "bronson_pext_bst_occ:" \
    "brown_ext_ist_lf:" \
    "brown_ext_abtree_lf:" \
    "guerraoui_ext_bst_ticket:" \
    "morrison_cbtree:" \
    "natarajan_ext_bst_lf:" \
#    "brown_ext_abtree_rq_lf:" \
#    "brown_ext_bslack_rq_lf:" \
#    "hash_chaining:-DIDX_HASH=1" \
#    "brown_ext_abtree_rq_lf:-DUSE_RANGE_QUERIES -DRQ_UNSAFE" \
#    "brown_ext_bslack_rq_lf:-DUSE_RANGE_QUERIES -DRQ_UNSAFE" \
#    "brown_ext_bst_rq_lf:-DUSE_RANGE_QUERIES -DRQ_UNSAFE" \
    "srivastava_abtree_mcs:" \
    "srivastava_abtree_pub:" \
    "winblad_catree:" \
    "wang_openbwtree:-Wno-invalid-offsetof -latomic:../ds/wang_openbwtree/" \
)

mkdir bin 2>/dev/null

make_workload_dict() {
    # compile the given workload and algorithm
    workload=$1
    name=`echo $2 | cut -d":" -f1`
    opts=`echo $2 | cut -d":" -f2`
    extra_src_dirs=`echo $2 | cut -d":" -f3-`
    opts_clean=`echo $opts | tr " " "." | tr "=" "-"`
    fname=bin/log.compile.temp.$workload.$name.${opts_clean}.out
    echo "arg1=$1 arg2=$2 workload=$workload name=$name opts=$opts extra_src_dirs=$extra_src_dirs"
    make clean workload="$workload" data_structure_name="$name" data_structure_opts="$opts" extra_src_dirs="$extra_src_dirs"
    make -j workload="$workload" data_structure_name="$name" data_structure_opts="$opts" extra_src_dirs="$extra_src_dirs" > $fname 2>&1
    if [ $? -ne 0 ]; then
        echo "Compilation FAILED for $workload $name $opts"
        mv $fname bin/log.compile.failure.$workload.$name.${opts_clean}.txt
    else
        echo "Compiled $workload $name $opts"
        mv $fname bin/log.compile.success.$workload.$name.${opts_clean}.txt
    fi
    make clean_objs workload="$workload" data_structure_name="$name" data_structure_opts="$opts" > $fname 2>&1
}
export -f make_workload_dict



################################################################################
#### BEGIN check for proper git cloning WITH SUBMODULES
################################################################################
for dir in "../tools" "../common/recordmgr" ; do
    error=0
    if [ ! -d "$dir" ]; then
        error=1
    else
        num_files_in_tools=$(ls "$dir" | wc -l)
        if [ "$num_files_in_tools" -eq "0" ]; then
            error=1
        fi
    fi
done
if [ "$error" -eq "1" ] ; then
    echo "================================================================================"
    echo "==== ERROR: CANNOT FIND SUBMODULES                                          ===="
    echo "================================================================================"
    echo "==== You most likely used the wrong git clone command...                    ===="
    echo "====                                                                        ===="
    echo "==== Note that a special argument is needed for cloning:                    ===="
    echo "==== git clone https://gitlab.com/trbot86/setbench.git --recurse-submodules ===="
    echo "================================================================================"
    echo
    exit 1
fi
################################################################################
#### END check for proper git cloning WITH SUBMODULES
################################################################################



rm -f bin/log.compile.*.txt

# check for gnu parallel
command -v parallel > /dev/null 2>&1
if [ "$?" -eq "0" ]; then
	parallel make_workload_dict ::: $workloads ::: "${algs[@]}"
else
	for workload in $workloads; do
	for alg in "${algs[@]}"; do
		make_workload_dict "$workload" "$alg"
	done
	done
fi

errorfiles=`ls bin/log.compile.failure* 2> /dev/null`
numerrorfiles=`ls bin/log.compile.failure* 2> /dev/null | wc -l`
if [ "$numerrorfiles" -ne "0" ]; then
    cat bin/log.compile.failure*
    echo "ERROR: some compilation command(s) failed. See the following file(s)."
    for x in $errorfiles ; do echo $(pwd)/$x ; done
    exit 1
else
    echo "Compilation successful."
fi
