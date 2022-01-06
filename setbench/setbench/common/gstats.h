/**
 * Usage goal:
 *  gstats_all_data::create_stat(name, optional aggregation_and_print_function)
 *       returns a STAT_ID, which I then #define
 *       also inserts name, gstats_stat_id and gstats_stat_id, name into dictionaries
 *       also saves gstats_stat_id, aggregation_and_print_function in a dict if it is set
 *  gstats_all_data::add_stat(STAT_ID, value)
 *  gstats_all_data::print_stat(STAT_ID, aggregation_and_print_function)
 *  gstats_all_data::print_all_stats()
 *       uses any aggregation_and_print_function specifications made in create_stat
 *
 * aggregation_and_print_functions:
 *  use them with lambdas to curry config args
 *  (could efficiently combine them in one loop over the data by making the agg_
 *   functions only contain the body of a loop over the data, then we can have
 *   a single aggregate function with an agg_ function argument that performs
 *   a loop, and runs all registered agg_ functions for a stat, one after another.
 *   printing will happen with separate print_ functions after the loop.
 *   declarations are still an issue, though, unless they're stored in a dict
 *   on the stack.)
 */

// TODO: add more output options, such as graphs!

/**
 * TODO: change stats so that handle_stats no longer takes gstats_output_items,
 *       and you can instead get stat aggregations using a function get(expr).
 *       the ultimate goal is to be able to provide complex expressions using
 *       the following grammar:
 *
 *         START = EXPR
 *          EXPR = FLOAT | STAT_NAME
 *          EXPR = [EXPR OPERATOR EXPR]
 *          EXPR = (AGGR GRAN EXPR)
 *      OPERATOR = + | - | * | / | %
 *          AGGR = SUM | AVG | MIN | MAX | COUNT | VARIANCE | STDEV | HIST_LOG | HIST_LIN
 *          GRAN = BY_INDEX | BY_THREAD | ALL
 *
 * This will allow invocations such as the following:
 *          get("3.0")                                                          -> returns array with a[0] = 3.0
 *          get("num_updates")                                                  -> returns array containing all values stored for num_updates (probably per-thread values)
 *          get("(SUM ALL num_updates)")                                        -> returns array with a[0] = the sum of num_updates over all threads&indices
 *          get("(SUM BY_THREAD num_updates)")                                  -> returns array containing per-thread sums of num_updates
 *          get("[(SUM ALL num_updates) / 3.0]")                                -> returns array with a[0] = the sum of num_updates over all threads&indices, all divided by 3.0
 *          get("[(SUM ALL num_updates) / " + str(elapsed_millis/1000.) + "]")  -> returns array with a[0] = throughput, which is computed as the sum of num_updates over all threads&indices, all divided by the number of seconds elapsed (assuming elapsed_millis is a local variable containing a number of milliseconds elapsed since some earlier time)
 *          get("[(SUM BY_THREAD num_updates) / " + str(elapsed_millis/1000.) + "]")        -> returns array containing per-thread throughputs
 *          get("[(STDEV BY_THREAD num_updates) / " + str(elapsed_millis/1000.) + "]")      -> returns array containing per-thread standard deviations for throughputs
 *          get("[[(STDEV BY_THREAD num_updates) / " + str(elapsed_millis/1000.) + "] / [(SUM BY_THREAD num_updates) / " + str(elapsed_millis/1000.) + "]]")    -> returns array containing per-thread standard deviations for throughputs, expressed as a percentage of the thread's throughput
 *          get("(HIST_LOG ALL range_query_sizes)")                             -> returns array representing a logarithmic histogram of the values in range_query_size over all threads&indices
 *          get("(HIST_LOG BY_THREAD range_query_sizes)")                       -> returns array containing per-thread arrays, each of which represents a logarithmic histogram of the values in range_query_size over all indices
 */

/**
 * Another attempt at a grammar for expressions:
 *         START = EXPR
 *          EXPR = FLOAT | STAT_NAME                    -- floating point number or stat name token
 *          EXPR = [EXPR OPERATOR EXPR]                 -- basic operator math (applies pairwise to array elements)
 *          EXPR = (REDUCE_OP GRANULARITY EXPR)         -- aggregating expression (reduce)
 *      OPERATOR = + | - | * | / | %
 *     REDUCE_OP = OPERATOR | min | max | avg | count | variance | stdev
 *     REDUCE_OP = hist_log | hist_lin
 *   GRANULARITY = by_index | by_thread | all
 */

// todo: later, expand this to work with tables of data, not just arrays indexed by thread and an arbitrary index (could use that arbitrary index to handle tables by concatenating table keys?)

/**
 * I've since realized that the evolution of ideas above basically mirrors the development of SQL databases.
 * I think on the output side, if I want an output module with fancier computational powers, I really just want to harness an in-memory SQL database like sqlite.
 */

#ifndef GSTATS_H
#define GSTATS_H

#include <thread>
#include <string>
#include <vector>
#include <map>
#include <cassert>
#include <cmath>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include "errors.h"
#include "locks_impl.h"
#include "plaf.h"

#ifndef VERBOSE
    #define VERBOSE if(0)
#endif
#define GSTATS_COMMA ,
#define GSTATS_THREAD_PADDING_BYTES 256
#define GSTATS_MAX_NUM_STATS 128
#ifndef GSTATS_MAX_THREAD_BUF_SIZE
#   define GSTATS_MAX_THREAD_BUF_SIZE (1<<22)
#endif
#define GSTATS_DATA_SIZE_BYTES 8
#define GSTATS_BITS_IN_BYTE 8
#define GSTATS_DEFAULT_HISTOGRAM_LIN_NUM_BUCKETS 10
#define GSTATS_DEFAULT_HISTOGRAM_LOG_NUM_BUCKETS GSTATS_DATA_SIZE_BYTES * GSTATS_BITS_IN_BYTE
#define GSTATS_SQ(x) ((x)*(x))
#define GSTATS_USE_TEMPLATE(id, func, args) (this->data_types[id] == LONG_LONG ? func<long long>(args) : func<double>(args))

typedef int gstats_stat_id;

enum gstats_enum_data_type {
    LONG_LONG,
    DOUBLE
};
enum gstats_enum_output_method {
    PRINT_RAW,
    PRINT_HISTOGRAM_LOG,
    PRINT_HISTOGRAM_LIN,
    PRINT_TO_FILE
};
enum gstats_enum_aggregation_function {
    NONE,
    FIRST,
    COUNT,
    MIN,
    MAX,
    SUM,
    AVERAGE,
    VARIANCE,
    STDEV
};
enum gstats_enum_aggregation_granularity {
    FULL_DATA,
    TOTAL,
    BY_INDEX,
    BY_THREAD
};

class gstats_output_item {
public:
    gstats_enum_output_method method;
    gstats_enum_aggregation_function func;
    gstats_enum_aggregation_granularity granularity;
    const char * const output_filename;
    int num_buckets_if_histogram_lin;
    gstats_output_item(gstats_enum_output_method method
                   , gstats_enum_aggregation_function func
                   , gstats_enum_aggregation_granularity granularity
                   , const char * const output_filename = NULL
                   , const int num_buckets_if_histogram_lin = GSTATS_DEFAULT_HISTOGRAM_LIN_NUM_BUCKETS)
            : method(method)
            , func(func)
            , granularity(granularity)
            , output_filename(output_filename)
            , num_buckets_if_histogram_lin(num_buckets_if_histogram_lin) {
        if (granularity == TOTAL && (method == PRINT_HISTOGRAM_LOG || method == PRINT_HISTOGRAM_LIN)) {
            setbench_error("cannot use granularity TOTAL with HISTOGRAM methods");
        }
        if ((method == PRINT_TO_FILE) && (func != NONE || granularity != FULL_DATA)) {
            setbench_error("PRINT_TO_FILE can only be used with aggregation function NONE, and granularity FULL_DATA");
        }
        if (output_filename && (method != PRINT_TO_FILE || func != NONE || granularity != FULL_DATA)) {
            setbench_error("output_filename can only be used with PRINT_TO_FILE, NONE, FULL_DATA");
        }
    }
};

class gstats_t {
private:
    class gstats_thread_data {
    public:
        PAD;
        char data[GSTATS_MAX_THREAD_BUF_SIZE]; // STORES ONLY LONG LONGS AND DOUBLES (8 bytes each)
        int offset[GSTATS_MAX_NUM_STATS];
        int capacity[GSTATS_MAX_NUM_STATS];
        int size[GSTATS_MAX_NUM_STATS];
        PAD;

        template <typename T>
        inline T * get_ptr(gstats_stat_id id) {
            return (T *) (data + offset[id]);
        }
    };
public:
    template <typename T>
    struct stat_metrics {
        T first;
        T cnt;
        T min;
        T max;
        T sum;
        T avg;
        T variance;
        T stdev;
        T none; // only used for histograms -- we simply processes all entries into the histogram (without aggregating), rather than plotting a histogram of aggregated data
        stat_metrics() {
            first = 0;
            cnt = 0;
            min = std::numeric_limits<T>::max();
            max = std::numeric_limits<T>::min();
            sum = 0;
            avg = 0;
            variance = 0;
            stdev = 0;
            none = 0;
        }
    };
private:
    #define GSTATS_FIELD_FIRST first
    #define GSTATS_FIELD_COUNT cnt
    #define GSTATS_FIELD_MIN min
    #define GSTATS_FIELD_MAX max
    #define GSTATS_FIELD_SUM sum
    #define GSTATS_FIELD_AVERAGE avg
    #define GSTATS_FIELD_VARIANCE variance
    #define GSTATS_FIELD_STDEV stdev
    #define GSTATS_FIELD_NONE none
    #define GSTATS_TYPE_TO_FIELD(type) GSTATS_FIELD_##type
    #define GSTATS_PRINT_LOWER(str) { std::string __s = str; transform(__s.begin(), __s.end(), __s.begin(), ::tolower); std::cout<<__s; }

    #define GSTATS_PRINT_AGG(agg_granularity_str, type, sid, metrics, num_metrics) { \
        GSTATS_PRINT_LOWER(#type); \
        std::cout<<"_"<<id_to_name[sid]<<agg_granularity_str<<"="; \
        if (metrics == NULL) { \
            std::cout<<std::endl; \
            for (int __tid=0;__tid<NUM_PROCESSES;++__tid) { \
                if (thread_data[__tid].size[sid]) { \
                    std::cout<<"thread "<<__tid; \
                    for (int __ix=0;__ix<thread_data[__tid].size[sid];++__ix) { \
                        if (get_stat<T>(__tid, sid, __ix) == std::numeric_limits<T>::max() || get_stat<T>(__tid, sid, __ix) == std::numeric_limits<T>::min()) { \
                            std::cout<<" "<<"0"; \
                        } else { \
                            std::cout<<" "<<get_stat<T>(__tid, sid, __ix); \
                        } \
                    } \
                    std::cout<<std::endl; \
                } \
            } \
        } else { \
            /* get last "empty" field so we can avoid printing lots of trailing zeros */ \
            int __lastEmpty = num_metrics; \
            for (int __i=num_metrics-1;__i>=0;--__i) { \
                if (metrics[__i].GSTATS_TYPE_TO_FIELD(type) == std::numeric_limits<T>::max() || metrics[__i].GSTATS_TYPE_TO_FIELD(type) == std::numeric_limits<T>::min() || metrics[__i].GSTATS_TYPE_TO_FIELD(type) == 0) { \
                    __lastEmpty = __i; \
                } else { \
                    break; \
                } \
            } \
            /* print the fields (metrics) */ \
            for (int __i=0;__i<__lastEmpty;++__i) { \
                if (metrics[__i].GSTATS_TYPE_TO_FIELD(type) == std::numeric_limits<T>::max() || metrics[__i].GSTATS_TYPE_TO_FIELD(type) == std::numeric_limits<T>::min()) { \
                    std::cout<<(__i?" ":"")<<"0"; \
                } else { \
                    std::cout<<(__i?" ":"")<<metrics[__i].GSTATS_TYPE_TO_FIELD(type); \
                } \
            } \
            std::cout<<std::endl; \
        } \
    }
    #define GSTATS_PRINT_HISTOGRAM_LOG(sid, agg_granularity_str, type, metrics, num_metrics) { \
        stat_metrics<long long> * __histogram = get_histogram_log<T>((sid), metrics, num_metrics); \
        int __first_nonzero = -1; \
        int __last_nonzero = 0; \
        for (int __i=0;__i<=GSTATS_DEFAULT_HISTOGRAM_LOG_NUM_BUCKETS;++__i) { \
            if (__histogram[__i].GSTATS_TYPE_TO_FIELD(type) > 0) { \
                __last_nonzero = __i; \
                if (__first_nonzero == -1) __first_nonzero = __i; \
            } \
        } \
        if (__first_nonzero == -1) __first_nonzero = 0; \
        std::cout<<std::endl<<"log_histogram_of_"; \
        GSTATS_PRINT_LOWER(#type); \
        std::cout<<"_"<<id_to_name[sid]<<agg_granularity_str<<"="; \
        for (int __i=0;__i<=__last_nonzero;++__i) std::cout<<(__i?" ":"")<<(1LL<<__i)<<":"<<__histogram[__i].GSTATS_TYPE_TO_FIELD(type); \
        std::cout<<std::endl; \
        for (int __i=__first_nonzero;__i<=__last_nonzero;++__i) { \
            std::cout<<"    "<<(__i?"(":"[")<<"2^"<<twoDigits(__i)<<", 2^"<<twoDigits(__i+1)<<"]: "<<__histogram[__i].GSTATS_TYPE_TO_FIELD(type)<<std::endl; \
        } \
    }
    #define GSTATS_PASTE2(a,b) a##b
    #define GSTATS_PASTE(a,b) GSTATS_PASTE2(a,b)
    #define GSTATS_PASTE_MIN(arg) GSTATS_PASTE(arg,_min)
    #define GSTATS_PASTE_MAX(arg) GSTATS_PASTE(arg,_max)
    #define GSTATS_PASTE_BUCKET_SIZE(arg) GSTATS_PASTE(arg,_bucket_size)
    #define GSTATS_PRINT_HISTOGRAM_LIN(sid, agg_granularity_str, type, metrics, num_metrics, num_buckets) { \
        std::pair<stat_metrics<long long> *, histogram_lin_dims> __p = get_histogram_lin<T>((sid), (num_buckets), metrics, num_metrics); \
        stat_metrics<long long> * __histogram = __p.first; \
        histogram_lin_dims __dims = __p.second; \
        auto __num_buckets = (__dims.GSTATS_PASTE_BUCKET_SIZE(GSTATS_TYPE_TO_FIELD(type)) < 1e-6) ? 0 : (num_buckets) - 1; \
        std::cout<<std::endl<<"linear_histogram_of_"; \
        GSTATS_PRINT_LOWER(#type); \
        std::cout<<"_"<<id_to_name[sid]<<agg_granularity_str<<"="; \
        for (int __i=0;__i<=__num_buckets;++__i) { \
            if (__histogram[__i].GSTATS_TYPE_TO_FIELD(type)) { \
                std::cout<<(__i?" ":"")<<(__dims.GSTATS_PASTE_MIN(GSTATS_TYPE_TO_FIELD(type)) + (1+__i)*__dims.GSTATS_PASTE_BUCKET_SIZE(GSTATS_TYPE_TO_FIELD(type)))<<":"<<__histogram[__i].GSTATS_TYPE_TO_FIELD(type); \
            } \
        } \
        std::cout<<std::endl; \
        for (int __i=0;__i<=__num_buckets;++__i) { \
            if (__histogram[__i].GSTATS_TYPE_TO_FIELD(type)) { \
                printf("    %s%12.2f, %12.2f]: %lld\n", (__i?"(":"["), (__dims.GSTATS_PASTE_MIN(GSTATS_TYPE_TO_FIELD(type)) + __i*__dims.GSTATS_PASTE_BUCKET_SIZE(GSTATS_TYPE_TO_FIELD(type))), (__dims.GSTATS_PASTE_MIN(GSTATS_TYPE_TO_FIELD(type)) + (1+__i)*__dims.GSTATS_PASTE_BUCKET_SIZE(GSTATS_TYPE_TO_FIELD(type))), __histogram[__i].GSTATS_TYPE_TO_FIELD(type)); \
            } \
        } \
    }

    PAD;
    const int NUM_PROCESSES;
    std::map<gstats_stat_id, std::string> id_to_name;
    std::map<std::string, gstats_stat_id> name_to_id;
    gstats_thread_data * thread_data;
    gstats_stat_id num_stats;

    std::multimap<gstats_stat_id, gstats_output_item> output_config;
//    PAD;

    std::vector<stat_metrics<double> *> arrays_to_delete;                        // to simplify deletion of heap allocated arrays
//    PAD;
    volatile int arrays_to_delete_lock = 0;
//    PAD;

    gstats_enum_data_type data_types[GSTATS_MAX_NUM_STATS];

//    PAD;
    stat_metrics<double> * computed_gstats_total[GSTATS_MAX_NUM_STATS];             // computed_gstats_total[gstats_stat_id][0] = stat_metrics
    stat_metrics<double> * computed_gstats_by_index[GSTATS_MAX_NUM_STATS];          // computed_gstats_by_index[gstats_stat_id][index] = stat_metrics
    stat_metrics<double> * computed_gstats_by_thread[GSTATS_MAX_NUM_STATS];         // computed_gstats_by_thread[gstats_stat_id][thread_id] = stat_metrics
//    PAD;
    int num_indices[GSTATS_MAX_NUM_STATS];                                         // num_indices[gstats_stat_id] = number of indices that have stat_metrics structs in computed_gstats_by_index[gstats_stat_id]
//    PAD;
    bool already_computed_stats;
    PAD;

public:
    gstats_t(const int num_processes)
            : NUM_PROCESSES(num_processes)
            , thread_data(new gstats_thread_data[num_processes])
            , num_stats(0)
    {
        assert(sizeof(double) == GSTATS_DATA_SIZE_BYTES);
        already_computed_stats = false;
        memset(computed_gstats_total, 0, GSTATS_MAX_NUM_STATS*sizeof(stat_metrics<double> *));
        memset(computed_gstats_by_index, 0, GSTATS_MAX_NUM_STATS*sizeof(stat_metrics<double> *));
        memset(computed_gstats_by_thread, 0, GSTATS_MAX_NUM_STATS*sizeof(stat_metrics<double> *));

        #pragma omp parallel for
        for (int i=0;i<num_processes;++i) {
            memset(&thread_data[i], 0, sizeof(gstats_thread_data));
        }
//         // parallel initialization
//         VERBOSE std::cout<<"parallel stat intialization: spawning "<<std::thread::hardware_concurrency()<<" threads"<<std::endl;
//         volatile bool start = false;

//         std::thread threads[num_processes];
//         for (int tid=0;tid<num_processes;++tid) {
//             threads[tid] = std::thread([tid,&start,num_processes,this]() {
// //                while (!start) { __sync_synchronize(); }
//                 //cout<<"thread "<<tid<<" initializing thread_data entry of size "<<sizeof(gstats_thread_data)<<std::endl;
//                 memset(&this->thread_data[tid], 0, sizeof(gstats_thread_data));
//             });
//         }
//         start = true;
//         __sync_synchronize();
//         VERBOSE std::cout<<"parallel stat initialization: joining "<<num_processes<<" threads"<<std::endl;
//         for (int tid=0;tid<num_processes;++tid) {
//             threads[tid].join();
//         }
//         VERBOSE std::cout<<"parallel stat initialization: joined all."<<std::endl;
    }

    ~gstats_t() {
#if !defined NO_CLEANUP_AFTER_WORKLOAD
        acquireLock(&arrays_to_delete_lock);
        for (auto it = arrays_to_delete.begin(); it != arrays_to_delete.end(); it++) {
            delete[] *it;
        }
        releaseLock(&arrays_to_delete_lock);
        delete[] thread_data;
#endif
    }

    template <typename T>
    void clear_to_value(gstats_stat_id id, T value) {
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            for (int i=0;i<thread_data[tid].capacity[id];++i) {
                set_stat(tid, id, value, i);
            }
        }
    }
    void clear_all() {
        #pragma omp parallel for
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            for (gstats_stat_id id=0;id<num_stats;++id) {
                memset(thread_data[tid].data + thread_data[tid].offset[id], 0, GSTATS_DATA_SIZE_BYTES*thread_data[tid].size[id]);
            }
            memset(thread_data[tid].size, 0, sizeof(int)*GSTATS_MAX_NUM_STATS);
        }
        for (gstats_stat_id id=0;id<num_stats;++id) {
            computed_gstats_total[id] = NULL;
            computed_gstats_by_index[id] = NULL;
            computed_gstats_by_thread[id] = NULL;
//                computed_histograms_log[id] = NULL;
//                computed_histograms_lin[id] = NULL;
            num_indices[id] = 0;
        }
        already_computed_stats = false;
    }

    gstats_stat_id create_stat(gstats_enum_data_type datatype, std::string name, const int capacity, std::initializer_list<gstats_output_item> gstats_output_items) {
        gstats_stat_id id = num_stats;

        data_types[id] = datatype;
        id_to_name[id] = name;
        name_to_id[name] = id;

        // save desired output items
        for (auto output_item : gstats_output_items) {
            output_config.insert(std::pair<gstats_stat_id, gstats_output_item>(id, output_item));
        }

        // initialize stat for all threads
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            thread_data[tid].offset[id] = (id == 0) ? 0 : thread_data[tid].offset[id-1] + thread_data[tid].capacity[id-1]*GSTATS_DATA_SIZE_BYTES;
            thread_data[tid].capacity[id] = capacity;
            auto endSize = thread_data[tid].offset[id] + thread_data[tid].capacity[id]*GSTATS_DATA_SIZE_BYTES;
            if (endSize > GSTATS_MAX_THREAD_BUF_SIZE) {
                std::cout<<"ERROR: stat w/id "<<id<<" ends at offset "<<endSize<<" in thread_data[tid].data, which runs off the end of the array. Either shrink your stats increase GSTATS_MAX_THREAD_BUF_SIZE in common/gstats.h."<<std::endl;
                exit(1);
            }
            thread_data[tid].size[id] = 0;
            //if (tid == 0) std::cout<<"stat id="<<id<<" name="<<name<<" tid="<<tid<<" offset="<<thread_data[tid].offset[id]<<" capacity="<<thread_data[tid].capacity[id]<<" size="<<thread_data[tid].size[id]<<" stat_ptr_addr="<<(long long) thread_data[tid].get_ptr<void>(id)<<std::endl;
            //if (tid == 0) std::cout<<"stat "<<id<<": "<<name<<" offset="<<thread_data[tid].offset[id]<<" capacity="<<thread_data[tid].capacity[id]<<std::endl;
        }

        num_stats++;
        if (num_stats > GSTATS_MAX_NUM_STATS) {
            std::cout<<"ERROR: added too many stats. either eliminate some stats or increase GSTATS_MAX_NUM_STATS in common/gstats.h."<<std::endl;
            exit(1);
        }
        return id;
    }

    template <typename T>
    inline T add_stat(const int tid, const gstats_stat_id id, T value, const int index) {
        if (index < 0 || index >= thread_data[tid].capacity[id]) {
            //error("index="<<index<<" >= capacity="<<thread_data[tid].capacity[id]<<" for tid="<<tid<<" sid="<<id<<" stat="<<id_to_name[id]);
            return -1;
        }
        assert(index >= 0 || index < thread_data[tid].capacity[id]);
        T * ptr = thread_data[tid].get_ptr<T>(id);
        T retval = (ptr[index] += value);
        //cout<<"adding to id="<<id<<" index="<<index<<" value="<<value<<" result="<<ptr[index]<<std::endl;
        if (index >= thread_data[tid].size[id]) {
            thread_data[tid].size[id] = index+1;
        }
        return retval;
    }

    template <typename T>
    inline T set_stat(const int tid, const gstats_stat_id id, T value, const int index) {
        T * ptr = thread_data[tid].get_ptr<T>(id);
        if (index >= thread_data[tid].capacity[id]) {
            //error("index="<<index<<" >= capacity="<<thread_data[tid].capacity[id]<<" for tid="<<tid<<" sid="<<id<<" stat="<<id_to_name[id]);
            return -1;
        }
        assert(index < thread_data[tid].capacity[id]);
        ptr[index] = value;
        //cout<<"adding to id="<<id<<" index="<<index<<" value="<<value<<" result="<<ptr[index]<<std::endl;
        if (index >= thread_data[tid].size[id]) {
            thread_data[tid].size[id] = index+1;
        }
        return value;
    }

    template <typename T>
    inline T append_stat(const int tid, const gstats_stat_id id, T value) {
        int index = thread_data[tid].size[id];
        if (index >= thread_data[tid].capacity[id]) {
            //error("index="<<index<<" >= capacity="<<thread_data[tid].capacity[id]<<" for tid="<<tid<<" sid="<<id<<" stat="<<id_to_name[id]);
            return -1;
        }
        T * ptr = thread_data[tid].get_ptr<T>(id);
        ptr[index] = value;
//            std::cout<<"appending to id="<<id<<" index="<<index<<" value="<<value<<" at index="<<index<<" result="<<ptr[index]<<std::endl;
        ++thread_data[tid].size[id];
        return value;
    }

    template <typename T>
    inline T get_stat(const int tid, const gstats_stat_id id, const int index) {
        if (index >= thread_data[tid].capacity[id]) {
            setbench_error("index="<<index<<" >= capacity="<<thread_data[tid].capacity[id]<<" for tid="<<tid<<" sid="<<id<<" stat="<<id_to_name[id]);
        }
        T * ptr = thread_data[tid].get_ptr<T>(id);
        return ptr[index];
    }

private:

    std::string twoDigits(int x) {
        std::stringstream ss;
        if (x >= 0 && x < 10) {
            ss<<"0";
        }
        ss<<x;
        return ss.str();
    }

    // TODO: implement more efficient memoization of each individual stat_metrics computation, below, so there are no duplicate computations, and no unnecessary computations
    // TODO: use a supplied predicate to ignore certain values, or make a distinction between initialized and uninitialized fields (currently it's just ignoring 0)

    template <typename T>
    stat_metrics<T> * compute_stat_metrics_total(const gstats_stat_id id) {
        stat_metrics<T> * results = new stat_metrics<T>[1];
        acquireLock(&arrays_to_delete_lock);
        arrays_to_delete.push_back((stat_metrics<double> *) results);
        releaseLock(&arrays_to_delete_lock);

        // first, cnt, min, max, sum
        results[0].first = (NUM_PROCESSES <= 0 || thread_data[0].size[id] <= 0) ? 0 : thread_data[0].get_ptr<T>(id)[0];
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            int size = thread_data[tid].size[id];
            auto data = thread_data[tid].get_ptr<T>(id);
            for (int ix=0;ix<size;++ix) {
                if (data[ix] == 0) continue;
                ++results[0].cnt;
                results[0].min = std::min(results[0].min, data[ix]);
                results[0].max = std::max(results[0].max, data[ix]);
                results[0].sum += data[ix];
            }
        }

        // avg
        if (results[0].cnt > 0) results[0].avg = results[0].sum / results[0].cnt;

        // variance
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            int size = thread_data[tid].size[id];
            auto data = thread_data[tid].get_ptr<T>(id);
            for (int ix=0;ix<size;++ix) {
                if (data[ix] == 0) continue;
                results[0].variance += GSTATS_SQ(data[ix] - results[0].avg);
            }
        }

        // unbiased sample stdev
        if (results[0].cnt > 1) results[0].stdev = sqrt(results[0].variance) / (results[0].cnt - 1);

        return results;
    }

    template <typename T>
    stat_metrics<T> * compute_stat_metrics_by_index(const gstats_stat_id id, int * num_indices) {
        // figure out how many indices there are,
        // so we know how many stat_metrics structs to allocate
        int cnt = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            int size = thread_data[tid].size[id];
            cnt = std::max(cnt, size);
        }

        stat_metrics<T> * results = new stat_metrics<T>[cnt];
        acquireLock(&arrays_to_delete_lock);
        arrays_to_delete.push_back((stat_metrics<double> *) results);
        releaseLock(&arrays_to_delete_lock);

        // first, cnt, min, max, sum, avg
        for (int ix=0;ix<cnt;++ix) {
            results[ix].first = (NUM_PROCESSES > 0) ? thread_data[0].get_ptr<T>(id)[ix] : 0;
            for (int tid=0;tid<NUM_PROCESSES;++tid) {
                int size = thread_data[tid].size[id];
                if (ix >= size) continue;
                auto data = thread_data[tid].get_ptr<T>(id);
                if (data[ix] == 0) continue;
                ++results[ix].cnt;
                results[ix].min = std::min(results[ix].min, data[ix]);
                results[ix].max = std::max(results[ix].max, data[ix]);
                results[ix].sum += data[ix];
            }
            if (results[ix].cnt > 0) results[ix].avg = results[ix].sum / results[ix].cnt;
        }

        // variance, stdev
        for (int ix=0;ix<cnt;++ix) {
            for (int tid=0;tid<NUM_PROCESSES;++tid) {
                int size = thread_data[tid].size[id];
                if (ix >= size) continue;
                auto data = thread_data[tid].get_ptr<T>(id);
                if (data[ix] == 0) continue;
                results[ix].variance += GSTATS_SQ(data[ix] - results[ix].avg);
            }
            if (results[ix].cnt > 1) results[ix].stdev = sqrt(results[ix].variance) / (results[ix].cnt - 1);
        }
        *num_indices = cnt;
        return results;
    }

    template <typename T>
    stat_metrics<T> * compute_stat_metrics_by_thread(const gstats_stat_id id) {
        stat_metrics<T> * results = new stat_metrics<T>[NUM_PROCESSES];
        acquireLock(&arrays_to_delete_lock);
        arrays_to_delete.push_back((stat_metrics<double> *) results);
        releaseLock(&arrays_to_delete_lock);

        // first, cnt, min, max, sum, avg
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            auto data = thread_data[tid].get_ptr<T>(id);
            results[tid].first = (thread_data[tid].size[id] > 0) ? data[0] : 0;
            for (int ix=0;ix<thread_data[tid].size[id];++ix) {
                if (data[ix] == 0) continue;
                ++results[tid].cnt;
                results[tid].min = std::min(results[tid].min, data[ix]);
                results[tid].max = std::max(results[tid].max, data[ix]);
                results[tid].sum += data[ix];
            }
            if (results[tid].cnt > 0) results[tid].avg = results[tid].sum / results[tid].cnt;
        }

        // variance, stdev
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            auto data = thread_data[tid].get_ptr<T>(id);
            for (int ix=0;ix<thread_data[tid].size[id];++ix) {
                if (data[ix] == 0) continue;
                results[tid].variance += GSTATS_SQ(data[ix] - results[tid].avg);
            }
            if (results[tid].cnt > 1) results[tid].stdev = sqrt(results[tid].variance) / (results[tid].cnt - 1);
        }
        return results;
    }

    template <typename T>
    int log2_capped(T x) {
        if (x <= 0) return -1;
        if (x <= 1) return 0;
        T log2_x = log2(x);
        int log2_x_i = (int) log2_x;
        //if (log2_x_i < 0 || log2_x_i > GSTATS_DEFAULT_HISTOGRAM_LOG_NUM_BUCKETS) std::cout<<"x="<<x<<" log2_x_i="<<log2_x_i<<std::endl;
#ifndef NDEBUG
        if (log2_x_i < 0) std::cout<<"log2_x_i="<<log2_x_i<<" log2_x="<<log2_x<<" x="<<x<<std::endl;
#endif
        assert(log2_x_i >= 0);
        return log2_x_i > GSTATS_DEFAULT_HISTOGRAM_LOG_NUM_BUCKETS
                ? GSTATS_DEFAULT_HISTOGRAM_LOG_NUM_BUCKETS
                : log2_x_i;
    }

    template <typename T>
    stat_metrics<long long> * get_histogram_log(const gstats_stat_id id, stat_metrics<T> * const metrics, const int num_metrics) {
        // compute histogram
        constexpr const int num_buckets = GSTATS_DEFAULT_HISTOGRAM_LOG_NUM_BUCKETS+1;
        stat_metrics<long long> * __histogram = new stat_metrics<long long>[num_buckets+1];
        acquireLock(&arrays_to_delete_lock);
        arrays_to_delete.push_back((stat_metrics<double> *) __histogram);
        releaseLock(&arrays_to_delete_lock);
        for (int i=0;i<num_buckets+1;++i) {
            memset(&__histogram[i], 0, sizeof(stat_metrics<long long>));
        }
        stat_metrics<long long> * histogram = &__histogram[1]; // handle -1s returned by log2_capped

        // (MOSTLY) SEQUENTIAL
        if (num_indices[id] <= (1<<16)) {
            if (metrics) {
                /**
                 * if metrics is non-null, compute histogram from metrics
                 */
                #pragma omp parallel
                {
                    #pragma omp single
                    {
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].first)].first; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].cnt)].cnt; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].min)].min; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].max)].max; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].sum)].sum; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].avg)].avg; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].variance)].variance; } }
                        #pragma omp task
                        { for (int ix=0;ix<num_indices[id];++ix) { ++histogram[log2_capped(metrics[ix].stdev)].stdev; } }
                    }
                }
            } else {
                /**
                 * if metrics is null, compute histogram from the full data
                 */
                for (int ix=0;ix<num_indices[id];++ix) {
                    for (int tid=0;tid<NUM_PROCESSES;++tid) {
                        ++histogram[log2_capped(get_stat<T>(tid, id, ix))].none;
                    }
                }
            }

        // aggressively parallel (for very large)
        } else {

            // parallel histogram construction
            VERBOSE std::cout<<"parallel compute histogram: spawning "<<std::thread::hardware_concurrency()<<" threads"<<std::endl;
            std::thread threads[std::thread::hardware_concurrency()];
            volatile bool start = false;
            for (int thread_id=0;thread_id<(int) std::thread::hardware_concurrency();++thread_id) {
                //cout<<"    parallel compute histogram: spawning thread "<<thread_id<<std::endl;
                threads[thread_id] = std::thread([thread_id, histogram, id, &start, num_buckets, metrics, this]() {
                    // while (!start) { __sync_synchronize(); }

                    // compute our slice of the indices
                    int slice_size = this->num_indices[id] / std::thread::hardware_concurrency();
                    int start_ix = slice_size * thread_id;
                    int end_ix = (thread_id == (int) std::thread::hardware_concurrency()-1) ? this->num_indices[id] : slice_size * (thread_id+1);

                    stat_metrics<long long> * __thread_histogram = new stat_metrics<long long>[num_buckets+1];
                    for (int i=0;i<num_buckets+1;++i) {
                        memset(&__thread_histogram[i], 0, sizeof(stat_metrics<long long>));
                    }
                    stat_metrics<long long> * thread_histogram = &__thread_histogram[1]; // handle -1s returned by log2_capped

                    // compute a thread-local histogram for only our slice of the indices
                    if (metrics) {
                        /**
                         * if metrics is non-null, compute histogram from metrics
                         */
                        for (int ix=start_ix;ix<end_ix;++ix) {
                            ++thread_histogram[log2_capped(metrics[ix].first)].first;
                            ++thread_histogram[log2_capped(metrics[ix].cnt)].cnt;
                            ++thread_histogram[log2_capped(metrics[ix].min)].min;
                            ++thread_histogram[log2_capped(metrics[ix].max)].max;
                            ++thread_histogram[log2_capped(metrics[ix].sum)].sum;
                            ++thread_histogram[log2_capped(metrics[ix].avg)].avg;
                            ++thread_histogram[log2_capped(metrics[ix].variance)].variance;
                            ++thread_histogram[log2_capped(metrics[ix].stdev)].stdev;
                        }

                        // use atomic primitives to add our histogram amounts to the shared array
                        for (int bucket=0;bucket<num_buckets;++bucket) {
                            __sync_fetch_and_add(&histogram[bucket].first, thread_histogram[bucket].first);
                            __sync_fetch_and_add(&histogram[bucket].cnt, thread_histogram[bucket].cnt);
                            __sync_fetch_and_add(&histogram[bucket].min, thread_histogram[bucket].min);
                            __sync_fetch_and_add(&histogram[bucket].max, thread_histogram[bucket].max);
                            __sync_fetch_and_add(&histogram[bucket].sum, thread_histogram[bucket].sum);
                            __sync_fetch_and_add(&histogram[bucket].avg, thread_histogram[bucket].avg);
                            __sync_fetch_and_add(&histogram[bucket].variance, thread_histogram[bucket].variance);
                            __sync_fetch_and_add(&histogram[bucket].stdev, thread_histogram[bucket].stdev);
                        }
                    } else {
                        /**
                         * if metrics is null, compute histogram from the full data
                         */
                        for (int ix=start_ix;ix<end_ix;++ix) {
                            for (int tid=0;tid<NUM_PROCESSES;++tid) {
                                ++thread_histogram[log2_capped(get_stat<T>(tid, id, ix))].none;
                            }
                        }
                        // use atomic primitives to add our histogram amounts to the shared array
                        for (int bucket=0;bucket<num_buckets;++bucket) {
                            __sync_fetch_and_add(&histogram[bucket].none, thread_histogram[bucket].none);
                        }
                    }

                    delete[] __thread_histogram;

                    //cout<<"    parallel compute histogram: thread "<<thread_id<<" terminated"<<std::endl;
                });
                //cout<<"    parallel compute histogram: spawned thread "<<thread_id<<std::endl;
            }
            __sync_synchronize();
            start = true;
            __sync_synchronize();
            VERBOSE std::cout<<"parallel compute histogram: joining "<<std::thread::hardware_concurrency()<<" threads"<<std::endl;
            for (int i=0;i<(int) std::thread::hardware_concurrency();++i) {
                //cout<<"    parallel compute histogram: joining thread "<<i<<std::endl;
                threads[i].join();
                //cout<<"    parallel compute histogram: joined thread "<<i<<std::endl;
            }
            VERBOSE std::cout<<"parallel compute histogram: joined all."<<std::endl;
        }

        return histogram;
    }

    struct histogram_lin_dims {
        double first_min;
        double first_max;
        double first_bucket_size;
        double cnt_min;
        double cnt_max;
        double cnt_bucket_size;
        double min_min;
        double min_max;
        double min_bucket_size;
        double max_min;
        double max_max;
        double max_bucket_size;
        double sum_min;
        double sum_max;
        double sum_bucket_size;
        double avg_min;
        double avg_max;
        double avg_bucket_size;
        double variance_min;
        double variance_max;
        double variance_bucket_size;
        double stdev_min;
        double stdev_max;
        double stdev_bucket_size;
        double none_min;
        double none_max;
        double none_bucket_size;
    };

    template <typename T>
    std::pair<stat_metrics<long long> *, histogram_lin_dims> get_histogram_lin(const gstats_stat_id id, const int num_buckets, stat_metrics<T> * const metrics, const int num_metrics) {
        // compute histogram
        stat_metrics<long long> * histogram = new stat_metrics<long long>[num_buckets];
        acquireLock(&arrays_to_delete_lock);
        arrays_to_delete.push_back((stat_metrics<double> *) histogram);
        releaseLock(&arrays_to_delete_lock);
        for (int i=0;i<num_buckets;++i) {
            memset(&histogram[i], 0, sizeof(stat_metrics<long long>));
        }
        histogram_lin_dims dims;

        if (metrics) {
            /**
             * if metrics is non-null, compute histogram from metrics
             */
            dims.first_min = std::numeric_limits<double>::max();
            dims.first_max = std::numeric_limits<double>::min();
            dims.cnt_min = std::numeric_limits<double>::max();
            dims.cnt_max = std::numeric_limits<double>::min();
            dims.min_min = std::numeric_limits<double>::max();
            dims.min_max = std::numeric_limits<double>::min();
            dims.max_min = std::numeric_limits<double>::max();
            dims.max_max = std::numeric_limits<double>::min();
            dims.sum_min = std::numeric_limits<double>::max();
            dims.sum_max = std::numeric_limits<double>::min();
            dims.avg_min = std::numeric_limits<double>::max();
            dims.avg_max = std::numeric_limits<double>::min();
            dims.variance_min = std::numeric_limits<double>::max();
            dims.variance_max = std::numeric_limits<double>::min();
            dims.stdev_min = std::numeric_limits<double>::max();
            dims.stdev_max = std::numeric_limits<double>::min();
            for (int ix=0;ix<num_indices[id];++ix) {
                dims.first_min = std::min(dims.first_min, (double) metrics[ix].first);
                dims.first_max = std::max(dims.first_max, (double) metrics[ix].first);
                dims.cnt_min = std::min(dims.cnt_min, (double) metrics[ix].cnt);
                dims.cnt_max = std::max(dims.cnt_max, (double) metrics[ix].cnt);
                dims.min_min = std::min(dims.min_min, (double) metrics[ix].min);
                dims.min_max = std::max(dims.min_max, (double) metrics[ix].min);
                dims.max_min = std::min(dims.max_min, (double) metrics[ix].max);
                dims.max_max = std::max(dims.max_max, (double) metrics[ix].max);
                dims.sum_min = std::min(dims.sum_min, (double) metrics[ix].sum);
                dims.sum_max = std::max(dims.sum_max, (double) metrics[ix].sum);
                dims.avg_min = std::min(dims.avg_min, (double) metrics[ix].avg);
                dims.avg_max = std::max(dims.avg_max, (double) metrics[ix].avg);
                dims.variance_min = std::min(dims.variance_min, (double) metrics[ix].variance);
                dims.variance_max = std::max(dims.variance_max, (double) metrics[ix].variance);
                dims.stdev_min = std::min(dims.stdev_min, (double) metrics[ix].stdev);
                dims.stdev_max = std::max(dims.stdev_max, (double) metrics[ix].stdev);
            }
            dims.first_bucket_size = ((dims.first_max - dims.first_min) / num_buckets);
            dims.cnt_bucket_size = ((dims.cnt_max - dims.cnt_min) / num_buckets);
            dims.min_bucket_size = ((dims.min_max - dims.min_min) / num_buckets);
            dims.max_bucket_size = ((dims.max_max - dims.max_min) / num_buckets);
            dims.sum_bucket_size = ((dims.sum_max - dims.sum_min) / num_buckets);
            dims.avg_bucket_size = ((dims.avg_max - dims.avg_min) / num_buckets);
            dims.variance_bucket_size = ((dims.variance_max - dims.variance_min) / num_buckets);
            dims.stdev_bucket_size = ((dims.stdev_max - dims.stdev_min) / num_buckets);
            dims.none_bucket_size = ((dims.none_max - dims.none_min) / num_buckets);

            for (int ix=0;ix<num_indices[id];++ix) {
                if (dims.first_bucket_size) ++histogram[(int) (metrics[ix].first / dims.first_bucket_size)].first;
                if (dims.cnt_bucket_size) ++histogram[(int) (metrics[ix].cnt / dims.cnt_bucket_size)].cnt;
                if (dims.min_bucket_size) ++histogram[(int) (metrics[ix].min / dims.min_bucket_size)].min;
                if (dims.max_bucket_size) ++histogram[(int) (metrics[ix].max / dims.max_bucket_size)].max;
                if (dims.sum_bucket_size) ++histogram[(int) (metrics[ix].sum / dims.sum_bucket_size)].sum;
                if (dims.avg_bucket_size) ++histogram[(int) (metrics[ix].avg / dims.avg_bucket_size)].avg;
                if (dims.variance_bucket_size) ++histogram[(int) (metrics[ix].variance / dims.variance_bucket_size)].variance;
                if (dims.stdev_bucket_size) ++histogram[(int) (metrics[ix].stdev / dims.stdev_bucket_size)].stdev;
            }
        } else {
            /**
             * if metrics is null, compute histogram from the full data
             */
            dims.none_min = std::numeric_limits<double>::max();
            dims.none_max = std::numeric_limits<double>::min();
            for (int tid=0;tid<NUM_PROCESSES;++tid) {
                for (int ix=0;ix<num_indices[id];++ix) {
                    auto s = (double) get_stat<T>(tid, id, ix);
                    if (s == 0) continue;
                    dims.none_min = std::min(dims.none_min, s);
                    dims.none_max = std::max(dims.none_max, s);
                }
            }
            dims.none_bucket_size = ((dims.none_max - dims.none_min) / num_buckets);
            for (int tid=0;tid<NUM_PROCESSES;++tid) {
                for (int ix=0;ix<num_indices[id];++ix) {
                    auto s = (double) get_stat<T>(tid, id, ix);
                    if (s == 0) continue;
                    if (dims.none_bucket_size > 1e-6) {
                        ++histogram[(int) ((s - dims.none_min) / dims.none_bucket_size)].none;
                    } else {
                        ++histogram[0].none;
                    }
                }
            }
        }
        return std::pair<stat_metrics<long long> *, histogram_lin_dims>(histogram, dims);
    }

    void compute_before_printing() {
        if (already_computed_stats) return;
//            std::cout<<"start compute_before_printing()..."<<std::endl;

        #pragma omp parallel
        {
            #pragma omp single nowait
            {
                for (int id=0;id<num_stats;++id) {
                    if (this->data_types[id] == LONG_LONG) {
                        #pragma omp task
                        { this->computed_gstats_total[id] = (stat_metrics<double> *) compute_stat_metrics_total<long long>(id); }
                        // WARNING: compute_stat_metrics_INDEX SETS this->num_indices[id], WHICH IS NEEDED FOR GET_HISTOGRAM_... CALLS AND print_all!
                        #pragma omp task
                        { this->computed_gstats_by_index[id] = (stat_metrics<double> *) compute_stat_metrics_by_index<long long>(id, &this->num_indices[id]); }
                        #pragma omp task
                        { this->computed_gstats_by_thread[id] = (stat_metrics<double> *) compute_stat_metrics_by_thread<long long>(id); }
                    } else {
                        #pragma omp task
                        { this->computed_gstats_total[id] = compute_stat_metrics_total<double>(id); }
                        // WARNING: compute_stat_metrics_INDEX SETS this->num_indices[id], WHICH IS NEEDED FOR GET_HISTOGRAM_... CALLS AND print_all!
                        #pragma omp task
                        { this->computed_gstats_by_index[id] = compute_stat_metrics_by_index<double>(id, &this->num_indices[id]); }
                        #pragma omp task
                        { this->computed_gstats_by_thread[id] = compute_stat_metrics_by_thread<double>(id); }
                    }
                }
            }
        }
        already_computed_stats = true;

        // // parallel statistics computation
        // VERBOSE std::cout<<"parallel stats compute before printing: spawning "<<(3*num_stats)<<" threads"<<std::endl;
        // std::thread threads[3*num_stats];
        // for (int id=0;id<num_stats;++id) {
        //     if (this->data_types[id] == LONG_LONG) {
        //         threads[3*id+0] = std::thread([this,id]() { this->computed_gstats_total[id] = (stat_metrics<double> *) compute_stat_metrics_total<long long>(id); });
        //         // WARNING: compute_stat_metrics_INDEX SETS this->num_indices[id], WHICH IS NEEDED FOR GET_HISTOGRAM_... CALLS AND print_all!
        //         threads[3*id+1] = std::thread([this,id]() { this->computed_gstats_by_index[id] = (stat_metrics<double> *) compute_stat_metrics_by_index<long long>(id, &this->num_indices[id]); });
        //         threads[3*id+2] = std::thread([this,id]() { this->computed_gstats_by_thread[id] = (stat_metrics<double> *) compute_stat_metrics_by_thread<long long>(id); });
        //     } else {
        //         threads[3*id+0] = std::thread([this,id]() { this->computed_gstats_total[id] = compute_stat_metrics_total<double>(id); });
        //         // WARNING: compute_stat_metrics_INDEX SETS this->num_indices[id], WHICH IS NEEDED FOR GET_HISTOGRAM_... CALLS AND print_all!
        //         threads[3*id+1] = std::thread([this,id]() { this->computed_gstats_by_index[id] = compute_stat_metrics_by_index<double>(id, &this->num_indices[id]); });
        //         threads[3*id+2] = std::thread([this,id]() { this->computed_gstats_by_thread[id] = compute_stat_metrics_by_thread<double>(id); });
        //     }
        // }
        // VERBOSE std::cout<<"parallel stats compute before printing: joining "<<(3*num_stats)<<" threads"<<std::endl;
        // for (int i=0;i<3*num_stats;++i) {
        //     threads[i].join();
        // }
        // VERBOSE std::cout<<"parallel stats compute before printing: joined all."<<std::endl;
        // __sync_synchronize();
        // already_computed_stats = true;
//            std::cout<<"finished compute_before_printing()."<<std::endl;
    }

public:

    template <typename T>
    T get_sum(const gstats_stat_id id) {
        T sum = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            int size = thread_data[tid].size[id];
            auto data = thread_data[tid].get_ptr<T>(id);
            for (int ix=0;ix<size;++ix) {
                if (data[ix] == 0) continue;
                sum += data[ix];
            }
        }
        return sum;
    }

    template <typename T>
    stat_metrics<T> * compute_stat_metrics(const gstats_stat_id id, gstats_enum_aggregation_granularity granularity) {
        switch (granularity) {
            case TOTAL:
                if (already_computed_stats) return (stat_metrics<T> *) computed_gstats_total[id];
                setbench_error("functionality disabled because it is very heavyweight, and is easy to misuse, biasing results. run print_stat() before calling this, instead.");
//                    else if (this->data_types[id] == LONG_LONG) return compute_stat_metrics_total<long long>(id);
//                    return compute_stat_metrics_total<double>(id);
            case BY_INDEX:
                if (already_computed_stats) return (stat_metrics<T> *) computed_gstats_by_index[id];
                setbench_error("functionality disabled because it is very heavyweight, and is easy to misuse, biasing results. run print_stat() before calling this, instead.");
//                    else if (this->data_types[id] == LONG_LONG) return compute_stat_metrics_by_index<long long>(id);
//                    return compute_stat_metrics_by_index<double>(id);
            case BY_THREAD:
                if (already_computed_stats) return (stat_metrics<T> *) computed_gstats_by_thread[id];
                setbench_error("functionality disabled because it is very heavyweight, and is easy to misuse, biasing results. run print_stat() before calling this, instead.");
//                    else if (this->data_types[id] == LONG_LONG) return compute_stat_metrics_by_thread<long long>(id);
//                    return compute_stat_metrics_by_thread<double>(id);
            default:
                setbench_error("should not get here");
                break;
        }
    }

    template <typename T>
    void print_stat(const gstats_stat_id id, gstats_output_item& output_item) {
        assert(id >= 0 && id < num_stats);

        //cout<<"printing stat "<<id_to_name[id]<<" with id "<<id<<std::endl;
        compute_before_printing();

        std::string granularity_str;
        stat_metrics<T> * metrics = NULL;
        int num_metrics = 0;
        switch (output_item.granularity) {
            case FULL_DATA:
                metrics = NULL;
                num_metrics = -1;
                granularity_str="_full_data";
                break;
            case TOTAL:
                metrics = (stat_metrics<T> *) computed_gstats_total[id];
                num_metrics = 1;
                granularity_str="_total";
                break;
            case BY_INDEX:
                metrics = (stat_metrics<T> *) computed_gstats_by_index[id];
                num_metrics = num_indices[id];
                granularity_str="_by_index";
                break;
            case BY_THREAD:
                metrics = (stat_metrics<T> *) computed_gstats_by_thread[id];
                num_metrics = NUM_PROCESSES;
                granularity_str="_by_thread";
                break;
        }

        if (output_item.func == NONE) {
            if (output_item.granularity != FULL_DATA) setbench_error("must use aggregation granularity FULL_DATA when using aggregation function NONE");
        }
        if (output_item.granularity == FULL_DATA) {
            if (output_item.func != NONE) setbench_error("must use aggregation function NONE when using aggregation granularity FULL_DATA");
        }

        switch (output_item.method) {
            case PRINT_RAW:
                {
                    switch (output_item.func) {
                        case FIRST:             GSTATS_PRINT_AGG(granularity_str, FIRST, id, metrics, num_metrics); break;
                        case COUNT:             GSTATS_PRINT_AGG(granularity_str, COUNT, id, metrics, num_metrics); break;
                        case MIN:               GSTATS_PRINT_AGG(granularity_str, MIN, id, metrics, num_metrics); break;
                        case MAX:               GSTATS_PRINT_AGG(granularity_str, MAX, id, metrics, num_metrics); break;
                        case SUM:               GSTATS_PRINT_AGG(granularity_str, SUM, id, metrics, num_metrics); break;
                        case AVERAGE:           GSTATS_PRINT_AGG(granularity_str, AVERAGE, id, metrics, num_metrics); break;
                        case VARIANCE:          GSTATS_PRINT_AGG(granularity_str, VARIANCE, id, metrics, num_metrics); break;
                        case STDEV:             GSTATS_PRINT_AGG(granularity_str, STDEV, id, metrics, num_metrics); break;
                        case NONE:              GSTATS_PRINT_AGG(granularity_str, NONE, id, metrics, num_metrics); break;
                        default:                setbench_error("should not reach here");
                    }
                } break;
            case PRINT_HISTOGRAM_LOG:
                {
                    if (output_item.granularity == TOTAL) setbench_error("aggregation granularity TOTAL should not be used with HISTOGRAM output (since the histogram will simply plot a single point)");
                    switch (output_item.func) {
                        case FIRST:             GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, FIRST, metrics, num_metrics); break;
                        case COUNT:             GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, COUNT, metrics, num_metrics); break;
                        case MIN:               GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, MIN, metrics, num_metrics); break;
                        case MAX:               GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, MAX, metrics, num_metrics); break;
                        case SUM:               GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, SUM, metrics, num_metrics); break;
                        case AVERAGE:           GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, AVERAGE, metrics, num_metrics); break;
                        case VARIANCE:          GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, VARIANCE, metrics, num_metrics); break;
                        case STDEV:             GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, STDEV, metrics, num_metrics); break;
                        case NONE:              GSTATS_PRINT_HISTOGRAM_LOG(id, granularity_str, NONE, metrics, num_metrics); break;
                        default:                setbench_error("should not reach here"); break;
                    }
                } break;
            case PRINT_HISTOGRAM_LIN:
                {
                    if (output_item.granularity == TOTAL) setbench_error("aggregation granularity TOTAL should not be used with HISTOGRAM output (since the histogram will simply plot a single point)");
                    switch (output_item.func) {
                        case FIRST:             GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, FIRST, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case COUNT:             GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, COUNT, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case MIN:               GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, MIN, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case MAX:               GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, MAX, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case SUM:               GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, SUM, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case AVERAGE:           GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, AVERAGE, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case VARIANCE:          GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, VARIANCE, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case STDEV:             GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, STDEV, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        case NONE:              GSTATS_PRINT_HISTOGRAM_LIN(id, granularity_str, NONE, metrics, num_metrics, output_item.num_buckets_if_histogram_lin); break;
                        default:                setbench_error("should not reach here"); break;
                    }
                } break;
            case PRINT_TO_FILE:
                {
                    assert(output_item.granularity == FULL_DATA);
                    assert(output_item.func == NONE);

                    // // is there any actual data to write?
                    // int rows_to_write = 0;
                    // for (int __tid=0;__tid<NUM_PROCESSES;++__tid) {
                    //     if (thread_data[__tid].size[id]) {
                    //         for (int __ix=0;__ix<thread_data[__tid].size[id];++__ix) {
                    //             rows_to_write += (get_stat<T>(__tid, id, __ix) != std::numeric_limits<T>::max()
                    //                            && get_stat<T>(__tid, id, __ix) != std::numeric_limits<T>::min());
                    //         }
                    //     }
                    // }

                    // // if so, create an output file
                    // if (rows_to_write) {
                        std::ofstream ofile;
                        if (output_item.output_filename) {
                            ofile.open(output_item.output_filename);
                        } else {
                            std::stringstream ss;
                            ss<<id_to_name[id];
                            ss<<".txt";
                            ofile.open(ss.str());
                        }

                        for (int __tid=0;__tid<NUM_PROCESSES;++__tid) {
                            if (thread_data[__tid].size[id]) {
                                for (int __ix=0;__ix<thread_data[__tid].size[id];++__ix) {
                                    ofile<<__tid;
                                    ofile<<" "<<__ix;
                                    if (get_stat<T>(__tid, id, __ix) == std::numeric_limits<T>::max() || get_stat<T>(__tid, id, __ix) == std::numeric_limits<T>::min()) {
                                        ofile<<" "<<"0";
                                    } else {
                                        ofile<<" "<<get_stat<T>(__tid, id, __ix);
                                    }
                                    ofile<<std::endl;
                                }
                            }
                        }
                        ofile.close();
                    // }
                } break;
            default: setbench_error("should not reach here"); break;
        }
    }

    void print_all() {
        for (auto it = output_config.begin(); it != output_config.end(); it++) {
            GSTATS_USE_TEMPLATE(it->first, print_stat, it->first GSTATS_COMMA it->second);
        }
    }

};

#endif /* GSTATS_H */

