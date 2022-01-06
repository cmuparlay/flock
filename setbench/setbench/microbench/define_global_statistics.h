/*
 * File:   define_global_statistics.h
 * Author: t35brown
 *
 * Created on August 5, 2019, 5:25 PM
 */

#ifndef GSTATS_OUTPUT_DEFS_H
#define GSTATS_OUTPUT_DEFS_H

#define USE_GSTATS
#ifndef __AND
#   define __AND ,
#endif
#define GSTATS_HANDLE_STATS(gstats_handle_stat) \
    gstats_handle_stat(LONG_LONG, key_gen_histogram, 200, { \
            gstats_output_item(PRINT_RAW, SUM, BY_INDEX) \
    }) \
    gstats_handle_stat(LONG_LONG, num_inserts, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_deletes, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_searches, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_rq, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, num_operations, 1, { \
            gstats_output_item(PRINT_RAW, SUM, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, AVERAGE, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, STDEV, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, SUM, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MIN, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MAX, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, size_checksum, 1, {}) \
    gstats_handle_stat(LONG_LONG, key_checksum, 1, {}) \
    gstats_handle_stat(LONG_LONG, prefill_size, 1, {}) \
    gstats_handle_stat(LONG_LONG, time_thread_terminate, 1, { \
            gstats_output_item(PRINT_RAW, FIRST, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, MIN, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MAX, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, time_thread_start, 1, { \
            gstats_output_item(PRINT_RAW, FIRST, BY_THREAD) \
      __AND gstats_output_item(PRINT_RAW, MIN, TOTAL) \
      __AND gstats_output_item(PRINT_RAW, MAX, TOTAL) \
    }) \
    gstats_handle_stat(LONG_LONG, timer_duration, 1, {}) \
    gstats_handle_stat(LONG_LONG, duration_all_ops, 1, { /* note: used by brown_ext_ist_lf */ \
            gstats_output_item(PRINT_RAW, SUM, TOTAL) \
    }) \


#endif /* GSTATS_OUTPUT_DEFS_H */
