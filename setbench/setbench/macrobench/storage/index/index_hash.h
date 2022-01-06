#pragma once 

#include "global.h"
#include "helper.h"
#include "index_base.h"

//TODO make proper variables private
// each BucketNode contains items sharing the same key
class BucketNode {
public: 
	BucketNode(KEY_TYPE key) {	init(key); };
	void init(KEY_TYPE key) {
		this->key = key;
		next = NULL;
		items = NULL;
	}
	KEY_TYPE 		key;
	// The node for the next key	
	BucketNode * 	next;	
	// NOTE. The items can be a list of items connected by the next pointer. 
	VALUE_TYPE 		items;
};

// BucketHeader does concurrency control of Hash
class BucketHeader {
public:
	void init();
	void insert_item(KEY_TYPE key, VALUE_TYPE item, int part_id);
	void read_item(KEY_TYPE key, VALUE_TYPE * item, const char * tname);
	BucketNode * 	first_node;
	uint64_t 		node_cnt;
	bool 			locked;
};

// TODO Hash index does not support partition yet.
class Index : public index_base
{
public:
    PAD; // padding after superclass layout
    
        RC 			init(uint64_t bucket_cnt, int part_cnt);
	RC 			init(int part_cnt, 
					table_t * table, 
					uint64_t bucket_cnt);
	bool 		index_exist(KEY_TYPE key); // check if the key exist.
	RC 			index_insert(KEY_TYPE key, VALUE_TYPE item, int part_id=-1);
	// the following call returns a single item
	RC	 		index_read(KEY_TYPE key, VALUE_TYPE * item, int part_id=-1);	
	RC	 		index_read(KEY_TYPE key, VALUE_TYPE * item,
							int part_id=-1, int thd_id=0);
        
        void initThread(const int tid);
        void deinitThread(const int tid);
private:
	void get_latch(BucketHeader * bucket);
	void release_latch(BucketHeader * bucket);
	
	// TODO implement more complex hash function
	uint64_t hash(KEY_TYPE key) {	return key % _bucket_cnt_per_part; }
	
	BucketHeader ** 	_buckets;
	uint64_t	 		_bucket_cnt;
	uint64_t 			_bucket_cnt_per_part;
};
