/* 
 * File:   all_indexes.h
 * Author: trbot
 *
 * Created on May 28, 2017, 4:33 PM
 */

#ifndef ALL_INDEXES_H
#define ALL_INDEXES_H

#include "config.h"

#if defined IDX_HASH
#   include "index_hash.h"
#else
#   include "index_adapter.h"
#endif

#endif /* ALL_INDEXES_H */
