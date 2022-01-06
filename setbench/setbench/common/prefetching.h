#ifndef _LINUX_PREFETCH_H
#define _LINUX_PREFETCH_H

static inline void prefetch_range(void *addr, size_t len)
{
//    char * cachelineAddr = (char *) addr;
//    char * end = cachelineAddr + len;
//    for (; cachelineAddr < end; cachelineAddr += 4*64) {
//        __builtin_prefetch(cachelineAddr, 1);
//    }
}

#endif
