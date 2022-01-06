#ifndef PAPI_UTIL_IMPL_H
#define PAPI_UTIL_IMPL_H
#include "papi_util.h"
#include "plaf.h"
#include <iostream>
#include <string>

int all_cpu_counters[] = {
#ifdef USE_PAPI
//    PAPI_L1_DCM, // works on amd17h
    PAPI_L2_DCM, // works on amd17h also
    PAPI_L3_TCM, // does not work on amd17h
    PAPI_TOT_CYC,
   PAPI_TOT_INS,
//    PAPI_RES_STL,
//    PAPI_TLB_DM,
#endif
};
std::string all_cpu_counters_strings[] = {
#ifdef USE_PAPI
//    "PAPI_L1_DCM",
    "PAPI_L2_TCM",
    "PAPI_L3_TCM",
    "PAPI_TOT_CYC",
   "PAPI_TOT_INS",
//    "PAPI_RES_STL",
//    "PAPI_TLB_DM",
#endif
};
#ifdef USE_PAPI
const int nall_cpu_counters = sizeof(all_cpu_counters) / sizeof(all_cpu_counters[0]);
#endif

#ifdef USE_PAPI
int event_sets[MAX_THREADS_POW2];
long long counter_values[nall_cpu_counters];
#endif

char *cpu_counter(int c) {
#ifdef USE_PAPI
    char counter[PAPI_MAX_STR_LEN];

    PAPI_event_code_to_name(c, counter);
    return strdup(counter);
#endif
    return NULL;
}

void papi_init_program(const int numProcesses){
#ifdef USE_PAPI
    if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
        fprintf(stderr, "Error: Failed to init PAPI\n");
        exit(2);
    }

    if (PAPI_thread_init(pthread_self) != PAPI_OK) {
       fprintf(stderr, "PAPI_ERROR: failed papi_thread_init()\n");
       exit(2);
    }
    for (int i=0;i<numProcesses;++i) event_sets[i]=PAPI_NULL;
    for (int i=0;i<nall_cpu_counters;++i) counter_values[i]= 0;
#endif
}

void papi_deinit_program() {
#ifdef USE_PAPI
    PAPI_shutdown();
#endif
}

void papi_create_eventset(int id){
#ifdef USE_PAPI
    int * event_set = &event_sets[id];
    int result;
    if ((result = PAPI_create_eventset(event_set)) != PAPI_OK) {
       fprintf(stderr, "PAPI_ERROR: thread %d cannot create event set: %s\n", id, PAPI_strerror(result));
       exit(2);
    }
    for (int i = 0; i < nall_cpu_counters; i++) {
        int c = all_cpu_counters[i];
        if ((result = PAPI_query_event(c)) != PAPI_OK) {
            // std::cout<<"warning: PAPI event "<<cpu_counter(c)<<" could not be successfully queried: "<<PAPI_strerror(result)<<std::endl;
            continue;
        }
        if ((result = PAPI_add_event(*event_set, c)) != PAPI_OK) {
            if (result != PAPI_ECNFLCT) {
                fprintf(stderr, "PAPI ERROR: thread %d unable to add event %s: %s\n", id, cpu_counter(c), PAPI_strerror(result));
                exit(2);
            }
            /* Not enough hardware resources, disable this counter and move on. */
            std::cout<<"warning: could not add PAPI event "<<cpu_counter(c)<<"... disabled it."<<std::endl;
            all_cpu_counters[i] = PAPI_END + 1;
        }
    }
#endif
}

void papi_start_counters(int id){
#ifdef USE_PAPI
    int * event_set = &event_sets[id];
    int result;
    if ((result = PAPI_start(*event_set)) != PAPI_OK) {
       fprintf(stderr, "PAPI ERROR: thread %d unable to start counters: %s\n", id, PAPI_strerror(result));
       std::cout<<"relevant event_set is for tid="<<id<<" and has value "<<(*event_set)<<std::endl;
       exit(2);
    }
#endif
}

void papi_stop_counters(int id){
#ifdef USE_PAPI
    int * event_set = &event_sets[id];
    long long values[nall_cpu_counters];
    for (int i=0;i<nall_cpu_counters; i++) values[i]=0;

    int r;

    /* Get cycles from hardware to account for time stolen by co-scheduled threads. */
    if ((r = PAPI_stop(*event_set, values)) != PAPI_OK) {
       fprintf(stderr, "PAPI ERROR: thread %d unable to stop counters: %s\n", id, PAPI_strerror(r));
       exit(2);
    }
    int j= 0;
    for (int i = 0; i < nall_cpu_counters; i++) {
        int c = all_cpu_counters[i];
        if (PAPI_query_event(c) != PAPI_OK)
            continue;
        __sync_fetch_and_add(&counter_values[j], values[j]);
        j++;
    }
    if ((r = PAPI_cleanup_eventset(*event_set)) != PAPI_OK) {
       fprintf(stderr, "PAPI ERROR: thread %d unable to cleanup event set: %s\n", id, PAPI_strerror(r));
       exit(2);
    }
    if ((r = PAPI_destroy_eventset(event_set)) != PAPI_OK) {
       fprintf(stderr, "PAPI ERROR: thread %d unable to destroy event set: %s\n", id, PAPI_strerror(r));
       exit(2);
    }
    if ((r = PAPI_unregister_thread()) != PAPI_OK) {
       fprintf(stderr, "PAPI ERROR: thread %d unable to unregister thread: %s\n", id, PAPI_strerror(r));
       exit(2);
    }
#endif
}
void papi_print_counters(long long num_operations){
#ifdef USE_PAPI
    int i, j;
    for (i = j = 0; i < nall_cpu_counters; i++) {
        int c = all_cpu_counters[i];
        if (PAPI_query_event(c) != PAPI_OK) {
            std::cout<<all_cpu_counters_strings[i]<<"=-1"<<std::endl;
            continue;
        }
        std::cout<<all_cpu_counters_strings[i]<<"="<<((double)counter_values[j]/num_operations)<<std::endl;
        //printf("%s=%.3f\n", cpu_counter(c), (double)counter_values[j]/num_operations);
        j++;
    }
#endif
}
#endif