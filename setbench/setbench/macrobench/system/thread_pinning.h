/* 
 * File:   thread_pinning.h
 * Author: tabrown
 *
 * Created on June 23, 2016, 6:52 PM
 * 
 * Used to configure and implement a thread pinning policy.
 * 
 * Instructions:
 *  1.  invoke configurePolicy, passing the # of logical processors,
 *      and a string that describes the desired thread binding policy,
 *      e.g., "1,2,3,8-11,4-7,0".
 *      the string contains the IDs of logical processors, or ranges of IDs,
 *      separated by commas. to skip thread binding, pass the empty string.
 *  2.  have each thread invoke bindThread.
 * [3.] OPTIONAL: you can confirm the binding for a given thread by invoking
 *      getActualBinding. you can also check whether all
 *      logical processors had at most one thread mapped to them by invoking
 *      isInjectiveMapping.
 */

#ifndef THREAD_PINNING_H
#define	THREAD_PINNING_H

#include <cassert>
#include <sched.h>
#include <iostream>
#include <stdlib.h>
#include <string>
#include "plaf.h"

namespace thread_pinning {
    
    extern cpu_set_t ** cpusets;
    extern int * customBinding;
    extern int numCustomBindings;

    /**
     * Public functions
     */
    
    void setbench_deinit(const int numThreads);
    void configurePolicy(const int numThreads, string policy);
    void bindThread(const int tid);
    int getActualBinding(const int tid);
    bool isInjectiveMapping(const int numThreads);

    /**
     * Private helper functions
     */

    static unsigned digits(unsigned x) {
        int d = 1;
        while (x > 9) {
            x /= 10;
            ++d;
        }
        return d;
    }

    // parse token starting at argv[ix],
    // place bindings for the token at the end of customBinding, and
    // return the index of the first character in the next token,
    //     or the size of the string argv if there are no further tokens.
    static unsigned parseToken(std::string argv, int ix) {
        // token is of one of following forms:
        //      INT
        //      INT-INT
        // and is either followed by "," is the end of the string.
        // we first determine which is the case

        // read first INT
        int ix2 = ix;
        while (ix2 < argv.size() && argv[ix2] != '.') ++ix2;
        string token = argv.substr(ix, ix2-ix+1);
        int a = atoi(token.c_str());

        // check if the token is of the first form: INT
        ix = ix+digits(a);              // first character AFTER first INT
        if (ix >= argv.size() || argv[ix] == '.') {

            // add single binding
            //cout<<"a="<<a<<std::endl;
            //customBinding.push_back(a);
            customBinding[numCustomBindings++] = a;

        // token is of the second form: INT-INT
        } else {
            assert(argv[ix] == '-');
            ++ix;                       // skip '-'

            // read second INT
            token = argv.substr(ix, ix2-ix+1);
            int b = atoi(token.c_str());
            //cout<<"a="<<a<<" b="<<b<<std::endl;

            // add range of bindings
            for (int i=a;i<=b;++i) {
                //customBinding.push_back(i);
                customBinding[numCustomBindings++] = i;
            }

            ix = ix+digits(b);          // first character AFTER second INT
        }
        // note: ix is the first character AFTER the last INT in the token
        // this is either a comma ('.') or the end of the string argv.
        return (ix >= argv.size() ? argv.size() : ix+1 /* skip '.' */);
    }

    // argv contains a custom thread binding pattern, e.g., "1,2,3,8-11,4-7,0"
    // threads will be bound according to this binding
    static void parseCustom(std::string argv) {
        numCustomBindings = 0;

        unsigned ix = 0;
        while (ix < argv.size()) {
            ix = parseToken(argv, ix);
        }
    }

    static void doBindThread(const int tid, const int nprocessors) {
        if (sched_setaffinity(0, CPU_ALLOC_SIZE(nprocessors), cpusets[tid%nprocessors])) { // bind thread to core
            cout<<"ERROR: could not bind thread "<<tid<<" to cpuset "<<cpusets[tid%nprocessors]<<std::endl;
            exit(-1);
        }
    }

}

#endif	/* THREAD_PINNING_H */

