/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   tsc.h
 * Author: Maya Arbel-Raviv
 *
 * Created on June 10, 2017, 12:21 PM
 */

#ifndef TSC_H
#define TSC_H

#include <stdint.h>

static inline uint64_t read_tsc(void)
{
    unsigned upper, lower;
    asm volatile ("rdtsc" : "=a"(lower), "=d"(upper)::"memory");
    return ((uint64_t)lower)|(((uint64_t)upper)<<32 );
}

#endif /* TSC_H */

