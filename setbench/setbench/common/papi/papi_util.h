/* 
 * File:   papi_util.h
 * Author: Maya Arbel-Raviv
 *
 * Created on June 29, 2016, 3:18 PM
 */

#ifndef PAPI_UTIL_H
#define PAPI_UTIL_H

#ifdef USE_PAPI
#   include <papi.h>
#endif

void papi_init_program(const int numProcesses);
void papi_deinit_program();
void papi_create_eventset(int id);
void papi_start_counters(int id);
void papi_stop_counters(int id);
void papi_print_counters(long long all_ops);

#endif /* PAPI_UTIL_H */

