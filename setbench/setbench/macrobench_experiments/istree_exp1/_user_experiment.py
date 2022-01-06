#!/usr/bin/python3
from _basic_functions import *
import numpy as np

def define_experiment(exp_dict, args):
    set_dir_run         (exp_dict, os.getcwd() + '/../../macrobench')
    set_dir_tools       (exp_dict, os.getcwd() + '/../../tools' )
    set_dir_compile     (exp_dict, os.getcwd() + '/../../macrobench')
    set_cmd_compile     (exp_dict, './compile.sh')

    binaries_to_algs = dict({
            'bin/rundb_YCSB_bronson_pext_bst_occ'   : 'bronson'
          , 'bin/rundb_YCSB_brown_ext_abtree_lf'    : 'abtree'
          , 'bin/rundb_YCSB_brown_ext_ist_lf'       : 'istree'
    })
    binaries_list = list(binaries_to_algs.keys())

    thread_count_list   = shell_to_list('cd ' + get_dir_tools(exp_dict) + ' ; ./get_thread_counts_numa_nodes.sh', exit_on_error=True)
    min_thread_count    = int(thread_count_list[0])
    max_thread_count    = int(thread_count_list[-1]) ## last list element
    thread_pinning_str  = '-pin 0-{}'.format(max_thread_count-1)

    ## compute a suitable initial database size that will work FOR ALL thread counts.
    ## this is needed to work around a *peculiarity* of DBx1000:
    ## the size must be divisible by every thread count you run!
    init_size = compute_suitable_initsize(thread_count_list, 100000000)

    add_run_param       (exp_dict, '__trials'           , [1, 2, 3])
    add_run_param       (exp_dict, 'binary'             , binaries_list)
    add_run_param       (exp_dict, 'zeta'               , [0.01, 0.1, 0.5])
    add_run_param       (exp_dict, 'rw_ratio'           , ['-r0.9 -w0.1'])
    add_run_param       (exp_dict, 'init_size'          , [init_size])
    add_run_param       (exp_dict, 'thread_count'       , thread_count_list)
    add_run_param       (exp_dict, 'thread_pinning'     , [thread_pinning_str])

    ## i like to have a testing mode (enabled with argument --testing) that runs for less time,
    ##  with fewer parameters (to make sure nothing will blow up before i run for hours...)
    if args.testing:
        add_run_param   (exp_dict, '__trials'           , [1])
        add_run_param   (exp_dict, 'zeta'               , [0.1])

        ## only test one or two thread counts
        if max_thread_count == min_thread_count:
            add_run_param(exp_dict, 'thread_count'      , [min_thread_count])
        else:
            add_run_param(exp_dict, 'thread_count'      , [min_thread_count, max_thread_count])

        ## smaller initial size for testing
        init_size = compute_suitable_initsize(thread_count_list, 10000000)
        add_run_param   (exp_dict, 'init_size'          , [init_size])

    ## test whether the OS has time utility, and whether it has the desired capabilities
    ## IF SO, we will extract numerous fields from it...
    time_cmd = '/usr/bin/time -f "[time_cmd_output] time_elapsed_sec=%e, faults_major=%F, faults_minor=%R, mem_maxresident_kb=%M, user_cputime=%U, sys_cputime=%S, percent_cpu=%P"'
    if not os_has_suitable_time_cmd(time_cmd):
        time_cmd = ''

    ## configure run command (newlines and extra spacing removed before this is run)
    set_cmd_run(exp_dict, '''
            ASAN_OPTIONS=new_delete_type_mismatch=0
            LD_PRELOAD=../lib/libjemalloc.so
            numactl --interleave=all
            timeout 120
            {time_cmd}
            ./{binary} -t{thread_count} -s{init_size} {rw_ratio} -z{zeta} {thread_pinning}
    '''.replace('{time_cmd}', time_cmd))

    add_data_field      (exp_dict, 'alg'                , extractor=get_alg_extractor(binaries_to_algs))
    add_data_field      (exp_dict, 'throughput'         , coltype='REAL', extractor=extract_summary_subfield, validator=is_positive)
    add_data_field      (exp_dict, 'run_time'           , coltype='REAL', extractor=extract_summary_subfield, validator=is_positive)
    add_data_field      (exp_dict, 'txn_cnt'            , coltype='INTEGER', extractor=extract_summary_subfield, validator=is_positive)
    add_data_field      (exp_dict, 'abort_cnt'          , coltype='INTEGER', extractor=extract_summary_subfield)
    add_data_field      (exp_dict, 'ixTotalTime'        , coltype='REAL', extractor=extract_summary_subfield, validator=is_positive)
    add_data_field      (exp_dict, 'ixThroughput'       , coltype='REAL', extractor=extract_summary_subfield, validator=is_positive)

    if time_cmd:
        add_data_field  (exp_dict, 'time_elapsed_sec'   , coltype='REAL', extractor=extract_time_subfield)
        add_data_field  (exp_dict, 'faults_major'       , coltype='INTEGER', extractor=extract_time_subfield)
        add_data_field  (exp_dict, 'faults_minor'       , coltype='INTEGER', extractor=extract_time_subfield)
        add_data_field  (exp_dict, 'mem_maxresident_kb' , coltype='INTEGER', extractor=extract_time_subfield)
        add_data_field  (exp_dict, 'user_cputime'       , coltype='REAL', extractor=extract_time_subfield)
        add_data_field  (exp_dict, 'sys_cputime'        , coltype='REAL', extractor=extract_time_subfield)
        add_data_field  (exp_dict, 'percent_cpu'        , coltype='INTEGER', extractor=extract_time_percent_cpu)

    ## render one legend for all plots (since the legend is the same for all)
    add_plot_set(exp_dict, name='legend.png', series='alg', x_axis='thread_count', \
            y_axis='throughput', plot_type='bars', \
            plot_cmd_args='--legend-only --legend-columns 3')

    ## render plots for several of the data_fields above
    for field in ['throughput', 'ixThroughput', 'run_time']:
        add_plot_set(exp_dict \
            , name=field+'-z{zeta}.png' \
            , title='z={zeta}: '+field \
            , varying_cols_list=['zeta'] \
            , series='alg', x_axis='thread_count', y_axis=field \
            , plot_type='bars' \
        )
        # ## we place the above legend on each HTML page by providing "legend_file"
        # add_page_set(exp_dict, image_files=field+'-z{zeta}.png', legend_file='legend.png')

    ## add page comparing zeta in columns vs the DIFFERENT DATA FIELDS ABOVE in rows
    add_page_set(exp_dict \
        , image_files='{row_field}-z{zeta}.png' \
        , name='txns_vs_index' \
        , column_field='zeta' \
        , row_field=['throughput', 'ixThroughput', 'run_time'] \
        , legend_file='legend.png')

    ## render plots for several of the data_fields specified for the time_cmd above
    ## (only if suitable time command support is found)
    if time_cmd:
        for field in ['time_elapsed_sec', 'mem_maxresident_kb', 'percent_cpu']:
            add_plot_set(exp_dict \
                , name=field+'-z{zeta}.png' \
                , title='z={zeta} threads: '+field \
                , varying_cols_list=['zeta'] \
                , series='alg', x_axis='thread_count', y_axis=field \
                , plot_type='bars'
            )
            add_page_set(exp_dict, image_files=field+'-z{zeta}.png', legend_file='legend.png')

        add_plot_set(exp_dict \
            , name='user_vs_sys-z{zeta}-n{thread_count}.png' \
            , title='z={zeta} n={thread_count}' \
            , varying_cols_list=['zeta', 'thread_count'] \
            , x_axis='alg', y_axis=['user_cputime', 'sys_cputime'] \
            , plot_type='bars', plot_cmd_args='--stacked --legend-include --legend-columns 2' \
        )
        add_page_set(exp_dict, image_files='user_vs_sys-z{zeta}-n{thread_count}.png')

        add_plot_set(exp_dict \
            , name='pagefaults-z{zeta}-n{thread_count}.png' \
            , title='z={zeta} n={thread_count}' \
            , varying_cols_list=['zeta', 'thread_count'] \
            , x_axis='alg', y_axis=['faults_major', 'faults_minor'] \
            , plot_type='bars', plot_cmd_args='--stacked --legend-include --legend-columns 2' \
        )
        add_page_set(exp_dict, image_files='pagefaults-z{zeta}-n{thread_count}.png')



## test whether the OS has time utility, and whether it has the desired capabilities
def os_has_suitable_time_cmd(time_cmd):
    s = shell_to_str(time_cmd + ' echo 2>&1', print_error=False)
    if isinstance(s, int):
        assert(s != 0)                          ## error code was returned (so it's non-zero...)
        assert(s != 124)                        ## not timeout command killing us
    else:
        assert(isinstance(s, str))
        if s.count(',') == time_cmd.count(','):      ## appropriate number of fields emitted by time util
            return True
    return False

## given a list of strings of form "X=Y",
#  find "field_name=..." and return the "...",
#  AFTER casting to the most appropriate type in {int,float,str}
def _kvlist_get(kvlist, field_name):
    if any([not item or item.count('=') != 1 for item in kvlist]):
        print(Back.RED+Fore.BLACK+'## ERROR: some item is not of the form "ABC=XYZ" in list {}'.format(kvlist)+Style.RESET_ALL)
        raise

    if not any ([item.split('=')[0] == field_name for item in kvlist]):
        print(Back.RED+Fore.BLACK+'## ERROR: field_name {} not found in list {}'.format(field_name, kvlist)+Style.RESET_ALL)
        raise

    for item in kvlist:
        k,v = item.split('=')
        if k == field_name: value = v

    try: result = int(value)
    except ValueError:
        try: result = float(value)
        except ValueError: result = str(value)
    return result

def extract_summary_subfield(exp_dict, file_name, field_name):
    kvlist = shell_to_list('grep summary {} | cut -d" " -f2- | tr -d " "'.format(file_name), sep=',')
    return _kvlist_get(kvlist, field_name)

def extract_time_subfield(exp_dict, file_name, field_name):
    kvlist = shell_to_list('grep time_cmd_output {} | tail -1 | cut -d" " -f2- | tr -d " "'.format(file_name), sep=',')
    return _kvlist_get(kvlist, field_name)

def extract_time_percent_cpu(exp_dict, file_name, field_name):
    kvlist = shell_to_list('grep time_cmd_output {} | tail -1 | cut -d" " -f2- | tr -d " "'.format(file_name), sep=',')
    return _kvlist_get(kvlist, field_name).replace('%', '')

def get_extractor_for_summary_subfield_numeric(subfield_ix):
    def curried_extractor(exp_dict, file_name, field_name):
        # s = shell_to_str('grep summary {file_name} | cut -d" " -f{subfield_ix}'.format(file_name=file_name, subfield_ix=subfield_ix))
        s = shell_to_str('grep summary {file_name} | cut -d" " -f{subfield_ix} | tr -d ",a-zA-Z_= "'.format(file_name=file_name, subfield_ix=subfield_ix))
        if '.' in s: return float(s)
        if not s:
            print(Back.RED+Fore.BLACK+'## ERROR: subfield_ix {} of field {} and file {} was empty'.format(subfield_ix, field_name, file_name)+Style.RESET_ALL)
        return int(s)
    return curried_extractor

def get_alg_extractor(binaries_dict):
    def curried_extractor(exp_dict, file_name, field_name):
        binary = grep_line(exp_dict, file_name, 'binary')
        return binaries_dict[binary]
    return curried_extractor

def extract_maxres(exp_dict, file_name, field_name):
    ## manually parse the maximum resident size from the output of `time` and add it to the data file
    maxres_kb_str = shell_to_str('grep "maxres" {} | cut -d" " -f6 | cut -d"m" -f1'.format(file_name))
    return float(maxres_kb_str) / 1000

## compute smallest number that is at least desired_size,
## AND has, as its unique prime factors,
## all unique prime factors across all numbers in thread_count_list
## (this is to manage a peculiarity of DBx1000... it crashes if the size isn't divisible by the thread count...)
def compute_suitable_initsize(thread_count_list, desired_size):
    # print('compute_suitable_initsize: thread_count_list={} desired_size={}'.format(thread_count_list, desired_size))

    factors = get_common_prime_factors(thread_count_list)
    fprod = 1
    for f in factors.keys():
        fprod *= (f**factors[f])

    # fprod = int(np.prod(list(factors)))
    div = desired_size // fprod
    remainder = desired_size % fprod
    # print('    fprod={} div={} remainder={}'.format(fprod, div, remainder))

    retval = fprod*div if not remainder else fprod*(div+1)
    # print('    retval={}'.format(retval))
    # sys.exit(1)
    return retval

def get_prime_factors_bruteforce(n):
    factors = dict()
    if n <= 1: return factors
    i = 2
    while i * i <= n:
        if n % i:
            i += 1
        else:
            n //= i
            if i not in factors.keys(): factors[i] = 1
            else: factors[i] += 1
    if n > 1:
        if n not in factors.keys(): factors[n] = 1
        else: factors[n] += 1
    return factors

def get_common_prime_factors(number_list):
    factors = dict()
    for t in number_list:
        t = int(t)
        factormap = get_prime_factors_bruteforce(t)
        # print('    factormap({})={}'.format(t, factormap))
        for f in factormap.keys():
            if f not in factors.keys(): factors[f] = factormap[f]
            else: factors[f] = max(factormap[f], factors[f])
    # print('    factors={}'.format(factors))
    return factors






# deprecated comment that i might want later...

## note: we include thread_count below as a "dummy" x-axis so "alg" can be the *series*,
#        and we can get a plot style consistent with the above throughput plots.
#        (you cannot have series without an x-axis.)
#        since the thread count is filtered to a single element,
#        no difference is actually visible on the x-axis...
