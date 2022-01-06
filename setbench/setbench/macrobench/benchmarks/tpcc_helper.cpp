#include "tpcc_helper.h"

drand48_data ** tpcc_buffer;

#define ZERO_MSB_64B_MASK 0x0FFFFFFFFFFFFFFF /* claim 4 MSBs for indexes to use as reserved bits */
//#define ZERO_MSB_64B_MASK 0x7FFFFFFFFFFFFFFF

#ifdef HASH_PRIMARY_KEYS

static inline uint64_t hash_murmur3(uint64_t v) {
    v ^= v>>33;
    v *= 0xff51afd7ed558ccdLLU;
    v ^= v>>33;
    v *= 0xc4ceb9fe1a85ec53LLU;
    v ^= v>>33;
    return v & ZERO_MSB_64B_MASK;
}
#else

static inline uint64_t hash_murmur3(uint64_t v) {
    return v;
}
#endif

uint64_t itemKey(uint64_t item_id) {
    return hash_murmur3(item_id);
}

uint64_t distKey(uint64_t d_id, uint64_t d_w_id) {
    return hash_murmur3(d_w_id*DIST_PER_WARE+d_id);
}

uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
    return hash_murmur3(distKey(c_d_id, c_w_id)*g_cust_per_dist+c_id);
}

uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
    return hash_murmur3(distKey(d_id, w_id)*g_cust_per_dist+o_id);
}

uint64_t neworderKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
    return orderlineKey(w_id, d_id, o_id);
}

uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
    return orderlineKey(w_id, d_id, o_id);
}

uint64_t orderline_wdKey(uint64_t w_id, uint64_t d_id) {
    return distKey(d_id, w_id);
}

uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id) {
    uint64_t key = 0;
    char offset = 'A';
    for (uint32_t i = 0; i<strlen(c_last); i++)
        key = (key<<2) + (c_last[i]-offset);
    key = key<<3;
    key += c_w_id*DIST_PER_WARE+c_d_id;
    return hash_murmur3(key);
}

static inline uint64_t
pow2roundup (uint64_t x) {
    if (x < 0) return 0;
    --x;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return x+1;
}

// TODO: check for hash/key collisions for all combinations of c_last, c_id, d_id, and w_id in the data
uint64_t custNPKey_ordered_by_cid(char * c_last, uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
//    uint64_t key = 0;
//    char offset = 'A';
//    for (uint32_t i = 0; i<strlen(c_last); i++)
//        key = (key<<2) + (c_last[i]-offset);
//    key = key<<3;
//    key += c_w_id*DIST_PER_WARE+c_d_id;

    uint64_t key = custNPKey(c_last, c_d_id, c_w_id);
    
    // round total number of customer ids to a power of 2,
    // then shift the key left to make space for a unique c_id
    key *= pow2roundup(g_cust_per_dist+1);
    key += c_id;
    return key;
    
//    // round total number of combinations of customer, warehouse and district
//    // to a power of 2, then shift the key left to make space for a unique
//    // w_id, d_id and c_id in the lower order bits.
//    key *= pow2roundup(g_cust_per_dist*DIST_PER_WARE*g_num_wh);
//    key += c_w_id*DIST_PER_WARE*g_cust_per_dist + c_d_id*g_cust_per_dist + c_id;
//    return key;
}

uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id) {
    return hash_murmur3(s_w_id*g_max_items+s_i_id);
}

uint64_t Lastname(uint64_t num, char* name) {
    static const char *n[] ={"BAR", "OUGHT", "ABLE", "PRI", "PRES",
                             "ESE", "ANTI", "CALLY", "ATION", "EING"};
    strcpy(name, n[num/100]);
    strcat(name, n[(num/10)%10]);
    strcat(name, n[num%10]);
    return strlen(name);
}

uint64_t RAND(uint64_t max, uint64_t thd_id) {
    int64_t rint64 = 0;
    lrand48_r(tpcc_buffer[thd_id], &rint64);
    return rint64%max;
}

uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id) {
    return x+RAND(y-x+1, thd_id);
}

uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id) {
    static bool C_255_init = false;
    static bool C_1023_init = false;
    static bool C_8191_init = false;
    static uint64_t C_255, C_1023, C_8191;
    int C = 0;
    switch (A) {
        case 255:
            if (!C_255_init) {
                C_255 = (uint64_t) URand(0, 255, thd_id);
                C_255_init = true;
            }
            C = C_255;
            break;
        case 1023:
            if (!C_1023_init) {
                C_1023 = (uint64_t) URand(0, 1023, thd_id);
                C_1023_init = true;
            }
            C = C_1023;
            break;
        case 8191:
            if (!C_8191_init) {
                C_8191 = (uint64_t) URand(0, 8191, thd_id);
                C_8191_init = true;
            }
            C = C_8191;
            break;
        default:
            M_ASSERT(false, "Error! NURand\n");
            exit(-1);
    }
    return (((URand(0, A, thd_id)|URand(x, y, thd_id))+C)%(y-x+1))+x;
}

uint64_t MakeAlphaString(int min, int max, char* str, uint64_t thd_id) {
    char char_list[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
                        'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
                        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A',
                        'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
    uint64_t cnt = URand(min, max, thd_id);
    for (uint32_t i = 0; i<cnt; i++)
        str[i] = char_list[URand(0L, 60L, thd_id)];
    for (int i = cnt; i<=max; i++) // made less-EQUAL to fix a problem with non-terminated strings in the original dbx1000 implementation
        str[i] = '\0';

    return cnt;
}

uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id) {

    uint64_t cnt = URand(min, max, thd_id);
    for (UInt32 i = 0; i<cnt; i++) {
        uint64_t r = URand(0L, 9L, thd_id);
        str[i] = '0'+r;
    }
    return cnt;
}

uint64_t wh_to_part(uint64_t wid) {
    assert(g_part_cnt<=g_num_wh);
    return wid%g_part_cnt;
}
