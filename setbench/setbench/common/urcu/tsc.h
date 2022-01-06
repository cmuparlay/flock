/**
 * Copyright 2015
 * Maya Arbel (mayaarl [at] cs [dot] technion [dot] ac [dot] il).
 * Adam Morrison (mad [at] cs [dot] technion [dot] ac [dot] il).
 *
 * This file is part of Predicate RCU.
 *
 * Predicate RCU is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Authors Maya Arbel and Adam Morrison
 */

#ifndef _TSC_H_
#define _TSC_H_

#include <stdint.h>

static inline uint64_t
read_tsc(void)
{
        unsigned upper, lower;
        asm volatile("rdtsc":"=a"(lower), "=d"(upper)::"memory");
        return ((uint64_t) lower) | (((uint64_t) upper) << 32);
}


#endif	/*_TSC_H_*/
