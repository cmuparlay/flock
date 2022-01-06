#!/usr/bin/python3
from _basic_functions import *

def define_experiment(exp_dict, args):
    set_dir_compile ( exp_dict, os.getcwd() + '/../../microbench' )   ## working directory where the compilation command should be executed
    set_dir_tools   ( exp_dict, os.getcwd() + '/../../tools' )        ## directory where the prerequisite tools library is located
    set_dir_run     ( exp_dict, os.getcwd() + '/bin' )                ## working directory where your program should be executed
    set_dir_data    ( exp_dict, os.getcwd() + '/data' )               ## directory for data files produced by individual runs of your program
    ## note: the above can be accessed using functions get_dir_[...]

    ##
    ## add parameters that you want your experiments to be run with.
    ## your program will be run once for each set of values in the CROSS PRODUCT of all parameters.
    ## (i.e., we will run your program with every combination of parameters)
    ##
    ## if you want to perform repeated trials of each experimental configuration, add a run_param called "__trial"
    ##     and specify a list of trial numbers (as below).
    ##
    ## (the run_param doesn't *need* to be called __trials exactly, but if it is called __trials exactly,
    ##     then extra sanity checks will be performed to verify, for example, that each data point in a graphical plot
    ##     represents the average of precisely as many experimental runs as there are entries in the __trials list.)
    ##

    add_run_param     ( exp_dict, 'INS_DEL_FRAC'    , ['0.0 0.0', '0.5 0.5', '20.0 10.0'] )
    add_run_param     ( exp_dict, 'MAXKEY'          , [2000000, 20000000] )
    add_run_param     ( exp_dict, 'DS_TYPENAME'     , ['brown_ext_ist_lf', 'brown_ext_abtree_lf', 'bronson_pext_bst_occ'] )
    add_run_param     ( exp_dict, 'thread_pinning'  , ['-pin ' + shell_to_str('cd ' + get_dir_tools(exp_dict) + ' ; ./get_pinning_cluster.sh', exit_on_error=True)] )
    add_run_param     ( exp_dict, '__trials'        , [1, 2, 3] )
    add_run_param     ( exp_dict, 'TOTAL_THREADS'   , [1] + shell_to_listi('cd ' + get_dir_tools(exp_dict) + ' ; ./get_thread_counts_numa_nodes.sh', exit_on_error=True) )

    ## i like to have a testing mode (enabled with argument --testing) that runs for less time,
    ##  with fewer parameters (to make sure nothing will blow up before i run for 12 hours...)
    if args.testing:
        add_run_param ( exp_dict, '__trials'        , [1] )
        add_run_param ( exp_dict, 'TOTAL_THREADS'   , [1] + shell_to_listi('cd ' + get_dir_tools(exp_dict) + ' ; ./get_thread_counts_max.sh', exit_on_error=True) )

    ##
    ## specify how to compile and run your program.
    ##
    ## you can use any of the parameters you defined above to dynamically replace {_tokens_like_this}.
    ## you can also get the directories saved above by using:
    ##      {__dir_compile}
    ##      {__dir_tools}
    ##      {__dir_run}
    ##      {__dir_data}
    ##
    ## the following replacement token is also defined for you:
    ##      {__step}            the number of runs done so far, padded to six digits with leading zeros
    ##
    ## your compile command will be executed in the compile directory above.
    ## your run command will be executed in the run directory above.
    ##

    set_cmd_compile     ( exp_dict, 'make bin_dir={__dir_run} -j' )

    if args.testing:
        set_cmd_run     ( exp_dict, 'LD_PRELOAD=../../../lib/libjemalloc.so timeout 300 numactl --interleave=all time ./{DS_TYPENAME}.debra -nwork {TOTAL_THREADS} -insdel {INS_DEL_FRAC} -k {MAXKEY} -t 100 {thread_pinning} -rq 0 -rqsize 1 -nrq 0' )
    else:
        set_cmd_run     ( exp_dict, 'LD_PRELOAD=../../../lib/libjemalloc.so timeout 300 numactl --interleave=all time ./{DS_TYPENAME}.debra -nwork {TOTAL_THREADS} -nprefill {TOTAL_THREADS} -insdel {INS_DEL_FRAC} -k {MAXKEY} -t 10000 {thread_pinning} -rq 0 -rqsize 1 -nrq 0' )

    ## pattern for output filenames. note 1: these files will be placed in {__dir_data}/. note 2: filenames cannot contain spaces.
    set_file_data   ( exp_dict, 'data{__step}.txt' )

    ##
    ## add data fields to be fetched from all output files.
    ## each of these becomes a column in a data table in a sqlite database.
    ##
    ## by default, a field "XYZ" will be fetched from each data file using extractor grep_line,
    ##      which greps (searches) for a line of the form "XYZ=[arbitrary string]"
    ##
    ## if your field is not stored in that format, you can specify a custom "extractor" function,
    ##      as we do in our example "get_maxres" BELOW, to extract the max resident size
    ##      from the 6th space-separated column of the output of the linux "time" command
    ##
    ## also note: each of these fields becomes a replacement token, e.g., {PAPI_L3_TCM}.
    ##
    ## the following special fields are defined for you and automatically added to the resulting sqlite data table:
    ##      {__step}            the number of runs done so far, padded to six digits with leading zeros
    ##      {__cmd_run}         your cmd_run string with any tokens replaced appropriately for this run
    ##      {__file_data}       the output filename for the current run's data
    ##      {__path_data}       the relative path to the output file for the current run's data
    ##      {__hostname}        the result of running the hostname command on the machine
    ##

    ## note: in the following, defaults are "validator=is_nonempty" and "extractor=grep_line"

    add_data_field ( exp_dict, 'total_throughput'  , coltype='INTEGER' , validator=is_positive )
    add_data_field ( exp_dict, 'PAPI_L3_TCM'       , coltype='REAL' )
    add_data_field ( exp_dict, 'PAPI_L2_TCM'       , coltype='REAL' )
    add_data_field ( exp_dict, 'PAPI_TOT_CYC'      , coltype='REAL' )
    add_data_field ( exp_dict, 'PAPI_TOT_INS'      , coltype='REAL' )
    add_data_field ( exp_dict, 'maxresident_mb'    , coltype='REAL'    , validator=is_positive , extractor=get_maxres ) ## note the custom extractor
    add_data_field ( exp_dict, 'tree_stats_height' , coltype='INTEGER' )
    add_data_field ( exp_dict, 'validate_result'   , coltype='TEXT'    , validator=is_equal('success') )
    add_data_field ( exp_dict, 'MILLIS_TO_RUN'     , coltype='TEXT'    , validator=is_positive )
    add_data_field ( exp_dict, 'RECLAIM'           , coltype='TEXT' )
    add_data_field ( exp_dict, 'POOL'              , coltype='TEXT' )

    ##
    ## add_plot_set() will cause a SET of plots to be rendered as images in the data directory.
    ##
    ## the precise SET of plots is defined by the fields included in varying_cols_list[].
    ##  we will iterate over all distinct combinations of values in varying_cols_list,
    ##  and will render a plot for each.
    ##
    ## note: a plot's title and filename can only use replacement {tokens} that correspond
    ##       to fields THAT ARE INCLUDED in varying_cols_list[]
    ##       (this is because only those tokens are well defined and unique PER PLOT)
    ##

    ## can do direct passthrough of command line args to the plotting script (see, e.g.,. ../../tools/plotbars.py to see what kind of customization of plots can be done this way)
    plot_cmd_args = '--legend-include --legend-columns 3'

    ## you can also specify a python file containing hooks for configuring matplotlib's plotting style:
    ##    just after importing, and just before and after plotting.
    ## (your callback/hook functions will be provided with the mpl, plt, fig and ax variables
    ##    for matplotlib's environment, plot, figure and axes, so you can customize as you like.)

    set_plot_style_hooks_file( exp_dict, os.getcwd() + '/' + '_user_plot_style.py' )
    ## if you prefer, you can set this PER-PLOT SET instead of globally, by providing argument plot_style_hooks_file below

    add_plot_set(
            exp_dict
          , name='throughput-u{INS_DEL_FRAC}-k{MAXKEY}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY}: Throughput vs thread count'
          , varying_cols_list=['INS_DEL_FRAC', 'MAXKEY']
          , series='DS_TYPENAME'
          , x_axis='TOTAL_THREADS'
          , y_axis='total_throughput'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )
    add_plot_set(
            exp_dict
          , name='l2miss-u{INS_DEL_FRAC}-k{MAXKEY}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY}: L2 misses/op vs thread count'
          , varying_cols_list=['INS_DEL_FRAC', 'MAXKEY']
          , series='DS_TYPENAME'
          , x_axis='TOTAL_THREADS'
          , y_axis='PAPI_L2_TCM'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )
    add_plot_set(
            exp_dict
          , name='l3miss-u{INS_DEL_FRAC}-k{MAXKEY}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY}: L3 misses/op vs thread count'
          , varying_cols_list=['INS_DEL_FRAC', 'MAXKEY']
          , series='DS_TYPENAME'
          , x_axis='TOTAL_THREADS'
          , y_axis='PAPI_L3_TCM'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )
    add_plot_set(
            exp_dict
          , name='cycles-u{INS_DEL_FRAC}-k{MAXKEY}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY}: Cycles/op vs thread count'
          , varying_cols_list=['INS_DEL_FRAC', 'MAXKEY']
          , series='DS_TYPENAME'
          , x_axis='TOTAL_THREADS'
          , y_axis='PAPI_TOT_CYC'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )
    add_plot_set(
            exp_dict
          , name='instructions-u{INS_DEL_FRAC}-k{MAXKEY}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY}: Instructions/op vs thread count'
          , varying_cols_list=['INS_DEL_FRAC', 'MAXKEY']
          , series='DS_TYPENAME'
          , x_axis='TOTAL_THREADS'
          , y_axis='PAPI_TOT_INS'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )

    ##
    ## we can also filter data to a single series before plotting it
    ##
    ## example: a plot with data filtered to single threaded runs
    ##

    ## note that with only a single series, there is no need for a legend (so we update the plot_cmd_args)
    plot_cmd_args = ''

    add_plot_set(
            exp_dict
          , name='sequential-u{INS_DEL_FRAC}-k{MAXKEY}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY}: Single threaded throughput'
          , filter='TOTAL_THREADS == 1'
          , varying_cols_list=['INS_DEL_FRAC', 'MAXKEY']
          , x_axis='DS_TYPENAME'
          , y_axis='total_throughput'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )

    ##
    ## rather than filtering the thread count to a single value,
    ## we could also make it part of the name, so one plot is produced for each thread count.
    ## moreover, supposing we only want plots for the minimum and maximum thread counts,
    ##     (where the min/max thread counts are obtained directly from our run_param that we added above)
    ##     then we can filter appropriately first...
    ##

    ## construct filter string
    p = get_run_param( exp_dict, 'TOTAL_THREADS' )
    min_threads = p[0]
    max_threads = p[len(p)-1]
    filter_string = 'TOTAL_THREADS in ({}, {})'.format(min_threads, max_threads)

    add_plot_set(
            exp_dict
          , name='maxresident-u{INS_DEL_FRAC}-k{MAXKEY}-n{TOTAL_THREADS}.png'
          , title='u={INS_DEL_FRAC} k={MAXKEY} n={TOTAL_THREADS}: Max resident size (MB)'
          , filter=filter_string, varying_cols_list=['INS_DEL_FRAC', 'MAXKEY', 'TOTAL_THREADS']
          , x_axis='DS_TYPENAME'
          , y_axis='maxresident_mb'
          , plot_type='bars'
          , plot_cmd_args=plot_cmd_args
    )

    ##
    ## we can also generate html web pages to display various layouts of plots
    ## more specifically, you can define pages (or sets of pages) that
    ##  arrange the plots specified above into html tables, either:
    ##  - by specifying explicitly that a particular PLOT belongs in a particular page, table, row, column, or
    ##  - by specifying that a PAGE should use particular FIELD to define tables, rows and columns
    ##

    add_page_set( exp_dict, image_files='throughput-u{INS_DEL_FRAC}-k{MAXKEY}.png' )
    ## note: the above is interpreted the same as:
    ##  add_page_set(
    ##          exp_dict
    ##        , image_files='throughput-u{INS_DEL_FRAC}-k{MAXKEY}.png'
    ##        , name='throughput'
    ##        , column_field='INS_DEL_FRAC'
    ##        , row_field='MAXKEY'
    ##  )
    ## the more verbose expression simply gives you more control over which fields are placed in rows and columns
    ##
    ## how this interpretation is done:
    ## the image_files string has its .png extension removed, then it is tokenized by hypens ('-'), and
    ##  - the first token serving becomes the name argument
    ##  - the second defines the columns in each table (column_field argument)
    ##  - the third defines the rows in each table (row_field argument)
    ##  - the fourth, if it exists, defines a SET of tables in the same page (table_field argument)
    ##  - any further tokens define a SET of pages (i.e., the cross product of the remaining tokens),
    ##    and the filename for each such page is suffixed with a particular assigment of those fields
    ##    (page_field_list argument -- a python list)

    add_page_set( exp_dict, image_files='maxresident-u{INS_DEL_FRAC}-k{MAXKEY}-n{TOTAL_THREADS}.png' )
    ## note: the above is interpreted the same as:
    ##  add_page_set(
    ##          exp_dict
    #$        , image_files='maxresident-u{INS_DEL_FRAC}-k{MAXKEY}-n{TOTAL_THREADS}.png'
    ##        , name='maxresident'
    ##        , table_field='TOTAL_THREADS'
    ##        , column_field='INS_DEL_FRAC'
    ##        , row_field='MAXKEY'
    ##  )

    add_page_set( exp_dict, image_files='l2miss-u{INS_DEL_FRAC}-k{MAXKEY}.png' )
    add_page_set( exp_dict, image_files='l3miss-u{INS_DEL_FRAC}-k{MAXKEY}.png' )
    add_page_set( exp_dict, image_files='cycles-u{INS_DEL_FRAC}-k{MAXKEY}.png' )
    add_page_set( exp_dict, image_files='instructions-u{INS_DEL_FRAC}-k{MAXKEY}.png' )
    add_page_set( exp_dict, image_files='sequential-u{INS_DEL_FRAC}-k{MAXKEY}.png' )

## extractor functions take as arguments: a file to load data from, and a field name
def get_maxres(exp_dict, file_name, field_name):
    ## manually parse the maximum resident size from the output of `time` and add it to the data file
    maxres_kb_str = shell_to_str('grep "maxres" {} | cut -d" " -f6 | cut -d"m" -f1'.format(file_name))
    return float(maxres_kb_str) / 1000
