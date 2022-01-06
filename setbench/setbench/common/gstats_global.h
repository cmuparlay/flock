/*
 * File:   stats_config.h
 * Author: trbot
 *
 * Created on July 12, 2017, 8:06 PM
 *
 * Usage:
 *  Define the macro GSTATS_HANDLE_STATS with a new line for each statistic you want
 *      to create BEFORE including this header. See the example below.
 *  Add gstats_output_item structs to the initializer list to configure
 *      gstats_t.print_all() to produce output for this statistic.
 *  Invoke the macro GSTATS_DECLARE_STATS_OBJECT to declare a gstats_t object
 *      (and make it globally visible).
 *  Invoke GSTATS_DECLARE_ALL_STAT_IDS to declare (and make globally visible)
 *      integer identifiers for each statistic defined in GSTATS_HANDLE_STATS.
 *  Invoke GSTATS_CREATE_ALL at run time before starting your experiments.
 *  Invoke GSTATS_ADD and/or GSTATS_ADD_IX to add some value to a statistic
 *      (when using statistics with multiple indices, for example, for different
 *       times in an execution, use GSTATS_ADD_IX to add to a specific index)
 *  You can clear all gathered stats by invoking GSTATS_CLEAR_ALL.
 *  Print all stats by invoking GSTATS_PRINT.
 *
 * Example GSTATS_HANDLE_STATS definition:
 * #define GSTATS_HANDLE_STATS(gstats_handle_stat) \
 *      gstats_handle_stat(visited_in_bags, 1, {gstats_output_item(PRINT_HISTOGRAM_LOG, SUM, BY_INDEX)}) \
 *      gstats_handle_stat(insertions, 1, {gstats_output_item(PRINT_RAW, SUM, TOTAL)}) \
 *      gstats_handle_stat(insertions_stdev, 1, {gstats_output_item(PRINT_RAW, STDEV, TOTAL)}) \
 *      gstats_handle_stat(insertions_per_thread, num_threads, {gstats_output_item(PRINT_RAW, SUM, BY_THREAD)}) \
 *      gstats_handle_stat(tree_size_over_time_histogram, 1000000, {gstats_output_item(PRINT_HISTOGRAM_LIN, SUM, BY_INDEX, 10)})
 *          note: in the last line, 10 is the desired number of buckets in the linear histogram
 */

#ifndef STATS_CONFIG_H
#define STATS_CONFIG_H

#ifdef USE_GSTATS

#ifndef GSTATS_HANDLE_STATS
#error "Must define GSTATS_HANDLE_STATS before including this file"
#endif

#include "gstats.h"
#include "locks_impl.h"

/**
 * CONFIGURATION
 */

#define GSTATS_OBJECT_PTR_NAME _stats_ptr
#define GSTATS_OBJECT_NAME _stats
#define GSTATS_DECLARE_STATS_OBJECT(max_num_processes) \
    /*PAD;*/ \
    gstats_t * const GSTATS_OBJECT_PTR_NAME = new gstats_t(max_num_processes); \
    gstats_t& GSTATS_OBJECT_NAME = *GSTATS_OBJECT_PTR_NAME; \
    /*PAD;*/
extern gstats_t& GSTATS_OBJECT_NAME;
#define GSTATS_DESTROY delete GSTATS_OBJECT_PTR_NAME

/**
 * DO NOT EDIT BELOW
 */

#define __DECLARE_STAT_ID(data_type, stat_name_token, stat_capacity, stats_output_items) int stat_name_token;
#define __DECLARE_EXTERN_STAT_ID(data_type, stat_name_token, stat_capacity, stats_output_items) extern int stat_name_token;
#define __CREATE_STAT(data_type, stat_name_token, stat_capacity, stats_output_items) \
    stat_name_token = GSTATS_OBJECT_NAME.create_stat(data_type, #stat_name_token, stat_capacity, stats_output_items);

#define GSTATS_DECLARE_ALL_STAT_IDS GSTATS_HANDLE_STATS(__DECLARE_STAT_ID);
#define GSTATS_DECLARE_EXTERN_ALL_STAT_IDS GSTATS_HANDLE_STATS(__DECLARE_EXTERN_STAT_ID)
#define GSTATS_CREATE_ALL GSTATS_HANDLE_STATS(__CREATE_STAT)

#define GSTATS_ADD_IX(tid, stat, val, index) GSTATS_OBJECT_NAME.add_stat<long long>(tid, stat, val, index)
#define GSTATS_ADD_IX_D(tid, stat, val, index) GSTATS_OBJECT_NAME.add_stat<double>(tid, stat, val, index)
#define GSTATS_ADD(tid, stat, val) GSTATS_ADD_IX(tid, stat, val, 0)
#define GSTATS_ADD_D(tid, stat, val) GSTATS_ADD_IX_D(tid, stat, val, 0)
#define GSTATS_SET_IX(tid, stat, val, index) GSTATS_OBJECT_NAME.set_stat<long long>(tid, stat, val, index)
#define GSTATS_SET_IX_D(tid, stat, val, index) GSTATS_OBJECT_NAME.set_stat<double>(tid, stat, val, index)
#define GSTATS_SET(tid, stat, val) GSTATS_OBJECT_NAME.set_stat<long long>(tid, stat, val, 0)
#define GSTATS_SET_D(tid, stat, val) GSTATS_OBJECT_NAME.set_stat<double>(tid, stat, val, 0)
#define GSTATS_GET_IX(tid, stat, index) GSTATS_OBJECT_NAME.get_stat<long long>(tid, stat, index)
#define GSTATS_GET_IX_D(tid, stat, index) GSTATS_OBJECT_NAME.get_stat<double>(tid, stat, index)
#define GSTATS_GET(tid, stat) GSTATS_OBJECT_NAME.get_stat<long long>(tid, stat, 0)
#define GSTATS_GET_D(tid, stat) GSTATS_OBJECT_NAME.get_stat<double>(tid, stat, 0)
#define GSTATS_APPEND(tid, stat, val) GSTATS_OBJECT_NAME.append_stat<long long>(tid, stat, val)
#define GSTATS_APPEND_D(tid, stat, val) GSTATS_OBJECT_NAME.append_stat<double>(tid, stat, val)
#define GSTATS_GET_STAT_METRICS(stat, aggregation_granularity) GSTATS_OBJECT_NAME.compute_stat_metrics<long long>(stat, aggregation_granularity)
#define GSTATS_GET_STAT_METRICS_D(stat, aggregation_granularity) GSTATS_OBJECT_NAME.compute_stat_metrics<long long>(stat, aggregation_granularity)
#define GSTATS_CLEAR_ALL GSTATS_OBJECT_NAME.clear_all()
#define GSTATS_CLEAR_VAL(stat, val) GSTATS_OBJECT_NAME.clear_to_value(stat, val)
#define GSTATS_PRINT GSTATS_OBJECT_NAME.print_all()

#define GSTATS_TIMER_RESET(tid, timer_stat) GSTATS_SET(tid, timer_stat, get_server_clock())
#define GSTATS_TIMER_ELAPSED(tid, timer_stat) (get_server_clock() - GSTATS_GET(tid, timer_stat))
/**
 * Warning: this macro uses a non-portable, GCC-specific ({}) enclosure.
 */
#define GSTATS_TIMER_SPLIT(tid, timer_stat) ({ \
    uint64_t ___curr = get_server_clock(); \
    uint64_t ___old = GSTATS_GET(tid, timer_stat); \
    GSTATS_SET(tid, timer_stat, ___curr); \
    (___curr - ___old); /* "return" value of ({}) enclosure */ \
})
#define GSTATS_TIMER_APPEND_ELAPSED(tid, timer_stat, target_stat) GSTATS_APPEND(tid, target_stat, GSTATS_TIMER_ELAPSED(tid, timer_stat))
#define GSTATS_TIMER_APPEND_SPLIT(tid, timer_stat, target_stat) GSTATS_APPEND(tid, target_stat, GSTATS_TIMER_SPLIT(tid, timer_stat))

/**
 * External declarations
 */

GSTATS_DECLARE_EXTERN_ALL_STAT_IDS;

#else

#define GSTATS_OBJECT_NAME
#define GSTATS_DECLARE_STATS_OBJECT(max_num_processes)
#define GSTATS_DESTROY

/**
 * DO NOT EDIT BELOW
 */

#define __DECLARE_STAT_ID(data_type, stat_name_token, stat_capacity, stats_output_items)
#define __DECLARE_EXTERN_STAT_ID(data_type, stat_name_token, stat_capacity, stats_output_items)
#define __CREATE_STAT(data_type, stat_name_token, stat_capacity, stats_output_items)

#define GSTATS_DECLARE_ALL_STAT_IDS
#define GSTATS_DECLARE_EXTERN_ALL_STAT_IDS
#define GSTATS_CREATE_ALL
#define GSTATS_ADD_IX(tid, stat, val, index)
#define GSTATS_ADD_IX_D(tid, stat, val, index)
#define GSTATS_ADD(tid, stat, val)
#define GSTATS_ADD_D(tid, stat, val)
#define GSTATS_SET_IX(tid, stat, val, index)
#define GSTATS_SET_IX_D(tid, stat, val, index)
#define GSTATS_SET(tid, stat, val)
#define GSTATS_SET_D(tid, stat, val)
#define GSTATS_GET_IX(tid, stat, index)
#define GSTATS_GET_IX_D(tid, stat, index)
#define GSTATS_GET(tid, stat)
#define GSTATS_GET_D(tid, stat)
#define GSTATS_APPEND(tid, stat, val)
#define GSTATS_APPEND_D(tid, stat, val)
#define GSTATS_CLEAR_ALL
#define GSTATS_CLEAR_VAL(stat, val)
#define GSTATS_PRINT

#define GSTATS_TIMER_RESET(tid, timer_stat)
#define GSTATS_TIMER_ELAPSED(tid, timer_stat)
/**
 * Warning: this macro uses a non-portable, GCC-specific ({}) enclosure.
 */
#define GSTATS_TIMER_SPLIT(tid, timer_stat)
#define GSTATS_TIMER_APPEND_ELAPSED(tid, timer_stat, target_stat)
#define GSTATS_TIMER_APPEND_SPLIT(tid, timer_stat, target_stat)

#endif

#endif /* STATS_CONFIG_H */

