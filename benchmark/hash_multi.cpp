// Transactions with multiple insert/delete/find operations
// WORK IN PROGRESS
// Currently works with regular locks but sometimes fails with lock-free locks.

#include <parlay/primitives.h>
#include <parlay/random.h>
#include <parlay/io.h>
#include <parlay/internal/get_time.h>
#include <parlay/internal/group_by.h>
#include "zipfian.h"
#include "parse_command_line.h"
#include "flock/flock.h"
#include "set.h"

// This is the keys used for testing, actually set type could support larger keys
using key_type = unsigned int;
using opt = char;
constexpr int max_block_size = 16;

// types for the hash table
using K = long;
using V = long;
Set<K,V> os;

void assert_key_exists(bool b) {
  if(!b) {
    std::cout << "key not found" << std::endl;
    abort();
  }
}

struct op {
  K key;
  Set<K,V>::slot* slot;
  opt optype;
  lock::lock_entry le;
};

struct tx {
  int read_count;
  int update_count;
  op reads[max_block_size];
  op updates[max_block_size];
};

memory_pool<tx> tx_pool;

// Acquire locks for updates, validate reads, apply writes.
// Recursive so the locks can be nested.
std::optional<int> acquire_locks_and_apply(int pos, tx* trans) {
  if (pos == 0) { // all write locks have been acquired
    int cnt = 0;
    // validate reads
    for (int i=0; i < trans->read_count; i++) {
      auto s = trans->reads[i].slot;
      os.find_at(s, trans->reads[i].key);
      if (!s->unchanged(trans->reads[i].le))
	if (!s->is_self_locked()) return -1000;
    }

    // apply updates
    for (int i=0; i < trans->update_count; i++) {
      op* tt = &trans->updates[i];
      if (tt->optype == 0) {
	if (os.insert_at(tt->slot, tt->key, 123)) cnt++;
      } else {
	if (os.remove_at(tt->slot, tt->key)) cnt--;
      }
    }
    return cnt;
  } else { // acquire next lock
    auto s = trans->updates[pos-1].slot;
    return s->try_lock_result([=] {
	auto r = acquire_locks_and_apply(pos-1, trans);
	if (r.has_value() && *r != -1000) return *r;
	else return -1000;
      });
  }
}

// apply a range of n operations atomically
template <typename Set>
int apply_block(key_type* keys, opt* op_types, size_t n, Set* t) {
  return with_epoch([&] {
      tx* trans = tx_pool.new_obj();
      // bool repeat[16];
      // for (int i=0; i < n; i++) repeat[i] = false;
      // for (int i = 0; i < n; i++) 
      //  	if (op_types[i] != 2 && !repeat[i])
      //  	  for (int j= 0; j < n; j++) 
      //  	    if (j != i && keys[i] == keys[j])
      //  	      repeat[j] = true;

      int round = 0;
      int delay = 100;
      constexpr int max_delay = 100000;
      
      while (true) {
	round++;
	trans->update_count = 0;
	trans->read_count = 0;
	bool aborted = false;
	for (int i=0; i < n; i++) {
	  auto* s = t->get_slot(keys[i]);
	  __builtin_prefetch (s);
	  if (true) { 
	    if (op_types[i] == 2) {
	      if (round > 100) s->wait_lock();
	      if (s->is_locked()) aborted = true;
	      auto le = s->lock_load();
	      trans->reads[trans->read_count++] = op{keys[i], s, 2, le};
	    } else {
	      trans->updates[trans->update_count++] = {keys[i], s, op_types[i], 0};
	    }
	  }
	}

	if (aborted) {
	  //delay = std::min((delay*3)/2,100000);
	  delay = std::min(delay*2,max_delay);
	  for (volatile int k = 0; k < delay/2; k++);
	  continue;
	}
	
	//auto r = acquire_locks_and_apply(trans->updates, trans->updates+trans->update_count, trans);
	auto r = acquire_locks_and_apply(trans->update_count, trans);
	if (r.has_value() && r != -1000) {
	  tx_pool.retire(trans);
	  return r.value();
	}

	for (volatile int k = 0; k < delay; k++);
	delay = std::min(delay*2,max_delay);
      }
      return 0;
    });
}

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-l] [-n <size>] [-r <rounds>]");
  size_t default_size = 100000000;
  // processes to run experiments with
  int p = P.getOptionIntValue("-p", parlay::num_workers());  

  int rounds = P.getOptionIntValue("-r", 1);

  // do fixed time experiment
  bool fixed_time = !P.getOption("-insert_find_delete");
  double trial_time = P.getOptionDoubleValue("-tt", 1.0);

  bool balanced_tree = P.getOption("-bt");
  
  // number of distinct keys (keys will be selected among 2n distinct keys)
  long n = P.getOptionIntValue("-n", default_size);
  long nn = fixed_time ? 2*n : n;

  // number of hash buckets for hash tables
  int buckets = P.getOptionIntValue("-bu", n);

  // shuffles the memory pool to break sharing of cache lines
  // by neighboring tree nodes
  bool shuffle = P.getOption("-shuffle");

  // verbose
  verbose = P.getOption("-v");

  // clear the memory pool between rounds
  bool clear = P.getOption("-clear");

  // only relevant to strict locks (not try locks)
  wait_before_retrying_lock = P.getOption("-wait");

  // number of samples 
  long m = P.getOptionIntValue("-m", fixed_time ? 20000000 * std::min(p, 100) : n);
    
  // check consistency, on by default
  bool do_check = ! P.getOption("-no_check");

  // use try locks 
  try_only = !P.getOption("-strict_lock");

  // run a trivial test
  bool init_test = P.getOption("-i"); // run trivial test

  // use zipfian distribution
  double zipfian_param = P.getOptionDoubleValue("-z", 0.0);
  bool use_zipfian = (zipfian_param != 0.0);
  
  // use numbers from 1...2n if dense otherwise sparse numbers
  bool use_sparse = !P.getOption("-dense"); 

  // print memory usage statistics
  bool stats = P.getOption("-stats");

  // for mixed update/query, the percent that are updates
  int update_percent = P.getOptionIntValue("-u", 20); 

  // number of operations in one atomic block
  int block_size = P.getOptionIntValue("-b", 4);
  if (block_size > max_block_size) {
    std::cout << "block size is too large, max allowed = " 
	      << max_block_size << std::endl;
    abort();
  }

  // generate 2*n unique numbers in random order
  parlay::sequence<key_type> a;
  if (use_sparse) {
    auto x = parlay::delayed_tabulate(1.2*nn,[&] (size_t i) {
					       return (key_type) parlay::hash64(i);}); 
    auto xx = parlay::remove_duplicates(x);
    auto y = parlay::random_shuffle(xx);
    a = parlay::tabulate(nn, [&] (size_t i) {return y[i]+1;});
  } else
    a = parlay::random_shuffle(parlay::tabulate(nn, [] (key_type i) {
						      return i+1;}));

  parlay::sequence<key_type> b;
  if (use_zipfian) { 
    Zipfian z(nn, zipfian_param);
    b = parlay::tabulate(m, [&] (int i) { return a[z(i)]; });
  } else
    b = parlay::tabulate(m, [&] (int i) {return a[parlay::hash64(i) % nn]; });

  // parlay::parallel_for(0, m, [&] (size_t i) { assert(b[i] != 0); });

  // initially set to all finds (0 = insert, 1 = delete, 2 = find)
  parlay::sequence<char> op_type(m, 2);

  if (update_percent > 0) {
    int cnt = 2 * 100/update_percent;
    op_type = parlay::tabulate(m, [&] (size_t i) -> char {
		    auto h = parlay::hash64(m+i);
		    if (h % cnt == 0) return 0; //insert 
		    else if (h % cnt == 1) return 1; //delete
		    else return 2; //find
		 });
  }
    
  parlay::internal::timer t;

  for (int i = 0; i < rounds; i++) {
    if (verbose) std::cout << "round " << i << std::endl;    

    auto tr = os.empty(buckets);

    if (do_check) {
      size_t len = os.check(tr);
      if (len != 0) {
	std::cout << "BAD LENGTH = " << len << std::endl;
      }
    }

    parlay::parallel_for(0, n, [&] (size_t i) {
	os.insert(tr, a[i], 123); });

    if (do_check) {
      size_t expected = n;
      size_t got = os.check(tr);
      if (expected != got) {
	std::cout << "expected " << expected
		  << " keys after insertion, found " << got << std::endl;
	abort();
      }
    }

    parlay::sequence<size_t> totals(p);
    parlay::sequence<long> addeds(p);
    size_t tx_cnt = m/(p*block_size);
    t.start();
    auto start = std::chrono::system_clock::now();
    std::atomic<bool> finish = false;
    parlay::parallel_for(0, p, [&] (size_t i) {
	int cnt = 0;
	size_t j = i*tx_cnt;
	size_t total = 0;
	long added = 0;
	while (true) {
	  //std::cout << j << ", " << block_size << std::endl;
	  // every once in a while check if time is over
	  if (cnt == 100) { 
	    cnt = 0;
	    auto current = std::chrono::system_clock::now();
	    double duration = std::chrono::duration_cast<std::chrono::seconds>(current - start).count();
	    if (duration > trial_time || finish) {
	      totals[i] = total;
	      addeds[i] = added;
	      return;
	    }
	  }
	  added += apply_block(&b[j*block_size], &op_type[j*block_size], block_size, &tr);
	  j++;
	  cnt++;
	  total += block_size;
	  // quit early if out of samples
	  if (j >= (i+1)*(tx_cnt-1)) {
	    finish = true;
	    totals[i] = total;
	    addeds[i] = added;
	    return;
	  }
	}}, 1);
    double duration = t.stop();

    if (finish && (duration < .5))
      std::cout << "warning out of samples, finished in "
		<< duration << " seconds" << std::endl;

    //std::cout << duration << " : " << trial_time << std::endl;
    std::stringstream ss;
    ss << "zipfian=" << zipfian_param;
    size_t num_ops = parlay::reduce(totals);
    std::cout << std::setprecision(4)
	      << P.commandName() << ","
	      << update_percent << "%update,"
      	      << "b=" << block_size << ","
	      << "n=" << n << ","
	      << "p=" << p << ","
	      << (!use_zipfian ? "uniform" : ss.str()) << ","
	      << num_ops / (duration * 1e6) << std::endl;

    if (do_check) {
      size_t final_cnt = os.check(tr);
      long updates = parlay::reduce(addeds);
      if (n + updates != final_cnt)
	std::cout << "bad size: intial size = " << n 
		  << ", added " << updates
		  << ", final size = " << final_cnt 
		  << std::endl;
    }
    parlay::parallel_for(0, nn, [&] (size_t i) { os.remove(tr, a[i]); });
    if (clear) {
      os.clear();
      //descriptor_pool.clear();
      //log_array_pool.clear();
    }
    if (stats) {
      if (clear) std::cout << "the following should be zero if no memory leak" << std::endl;
      //descriptor_pool.stats();
      //log_array_pool.stats();
      os.stats();
    }
    os.retire(tr);
  }
}
