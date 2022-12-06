#include <string>
#include <iostream>
#include <sstream>
#include <limits>

#include <parlay/primitives.h>
#include <parlay/random.h>
#include <parlay/io.h>
#include <parlay/internal/get_time.h>
#include <parlay/internal/group_by.h>
#include "zipfian.h"
#include "parse_command_line.h"

void assert_key_exists(bool b) {
  if(!b) {
    std::cout << "key not found" << std::endl;
    abort();
  }
}

void print_array(bool* a, int N) {
  for(int i = 0; i < N; i++)
    std::cout << a[i];
  std::cout << std::endl;
}

template <typename SetType>
void test_persistence_concurrent(SetType& os) {
  int N = 1000;
  auto tr = os.empty(N);

  auto a = parlay::random_shuffle(parlay::tabulate(N, [&] (size_t i) {return i+1;}));
  std::atomic<bool> done = false;

  int num_threads = 2;

  parlay::parallel_for(0, num_threads, [&] (size_t tid) {
    if(tid == num_threads-1) {  // update thread
      std::cout << "starting to insert" << std::endl;
      for(int i = 0; i < N; i++) 
        os.insert(tr, a[i], i+1);
      std::cout << "starting to delete" << std::endl;
      for(int i = N-1; i >= 0; i--)
        os.remove(tr, a[i]);
      std::cout << "done updating" << std::endl;
      done = true;
    } else {  // query threads
      std::cout << "starting to query" << std::endl;
      int counter = 0;
      int counter2 = 0;
      while(!done) {
        bool seen[N+1];
        int max_seen;
	      vl::with_snapshot([&] {
          max_seen = -1;
          for(int i = 1; i <= N; i++) seen[i] = false;
          for(int i = 1; i <= N; i++) {
            auto val = os.find_(tr, i);
            if(val.has_value()) {
              seen[val.value()] = true;
              max_seen = std::max(max_seen, (int) val.value());
            }
          }
        });
        std::cout << "max_seen: " << max_seen << std::endl;
        // print_array(seen, N);
        for(int i = 1; i <= max_seen; i++)
          if(!seen[i]) {
            std::cout << "inconsistent snapshot" << std::endl;
            break;
            abort();
          }
        counter2++;
        if(max_seen > 2 && max_seen < N-3) 
          counter++; // saw an intermediate state
        // if(counter2 > 10 && counter == 0) break;
      }
      if(counter < 3) {
        std::cout << "not enough iterations by query thread" << std::endl;
        // abort();
      }
    } }, 1);
  os.retire(tr);
// #endif
}

template <typename SetType, typename Tree, typename Range>
void insert_balanced(SetType& os, Tree& tr, Range A) {
  if (A.size() == 0) return;
  size_t mid = A.size()/2;
  os.insert(tr, A[mid], 123);
  parlay::par_do([&] {insert_balanced(os, tr, A.cut(0,mid));},
                 [&] {insert_balanced(os, tr, A.cut(mid+1,A.size()));});
}

enum op_type : char {Find, Insert, Remove, Range, MultiFind};

template <typename SetType>
void test_sets(SetType& os, size_t default_size, commandLine P) {
  // processes to run experiments with
  int p = P.getOptionIntValue("-p", parlay::num_workers());  

  int rounds = P.getOptionIntValue("-r", 1);

  // do fixed time experiment
  bool fixed_time = !P.getOption("-insert_find_delete");
  double trial_time = P.getOptionDoubleValue("-tt", 1.0);

  bool balanced_tree = P.getOption("-bt");
  int range_size = P.getOptionIntValue("-rs",16);
  int range_percent = P.getOptionIntValue("-range",0);
  int multifind_percent = P.getOptionIntValue("-mfind",0);
  
#ifndef Range_Search
  if (range_percent > 0) {
    std::cout << "range search not implemented for this structure" << std::endl;
    return;
  }
#endif
  
  // number of distinct keys (keys will be selected among 2n distinct keys)
  long n = P.getOptionIntValue("-n", default_size);
  long nn = fixed_time ? 2*n : n;

  // number of hash buckets for hash tables
  int buckets = P.getOptionIntValue("-bu", n);

  // shuffles the memory pool to break sharing of cache lines
  // by neighboring list/tree nodes
  bool shuffle = P.getOption("-shuffle");
  bool initialize_with_deletes = P.getOption("-id");

  // verbose
  bool verbose = P.getOption("-v");

  // clear the memory pool between rounds
  bool clear = P.getOption("-clear");

  // number of samples 
  long m = P.getOptionIntValue("-m", fixed_time ? (long) (trial_time * 5000000 * std::min(p, 100)) : n);
    
  // check consistency, on by default
  bool do_check = ! P.getOption("-no_check");

  // use try locks 
  // try_only = !P.getOption("-strict_lock");

  // run a trivial test
  bool init_test = P.getOption("-i"); // run trivial test

  // use zipfian distribution
  double zipfian_param = P.getOptionDoubleValue("-z", 0.0);
  bool use_zipfian = (zipfian_param != 0.0);
  
  // use numbers from 1...2n if dense otherwise sparse numbers
  bool use_sparse = !P.getOption("-dense");
#ifdef Dense_Keys  // for range queries on hash tables
  if (range_percent > 0)
    use_sparse = false;
#endif

  // print memory usage statistics
  bool stats = P.getOption("-stats");

  // for mixed update/query, the percent that are updates
  int update_percent = P.getOptionIntValue("-u", 20); 

  if (init_test) {  // trivial test inserting 4 elements and deleting one
    std::cout << "running sanity checks" << std::endl;
    auto tr = os.empty(4);
    // os.print(tr);
    os.insert(tr, 3, 123);
    // os.print(tr);
    os.insert(tr, 7, 123);
    // os.print(tr);
    os.insert(tr, 1, 123);
    // os.print(tr);
    os.insert(tr, 11, 123);
    // os.print(tr);
    os.remove(tr, 3);
    // os.print(tr);
    assert_key_exists(os.find(tr, 7).has_value());
    assert_key_exists(os.find(tr, 1).has_value());
    assert_key_exists(os.find(tr, 11).has_value());
    assert(!os.find(tr, 10).has_value());
    assert(!os.find(tr, 3).has_value());

    // #ifdef Multi_Find
    vl::with_snapshot([&] {
      assert_key_exists(os.find_(tr, 7).has_value());
      assert_key_exists(os.find_(tr, 1).has_value());
      assert_key_exists(os.find_(tr, 11).has_value());
      assert(!os.find_(tr, 10).has_value());
      assert(!os.find_(tr, 3).has_value());
      });
    // #endif

    // os.print(tr);
    // std::cout << "size = " << os.check(tr) << std::endl;
    os.retire(tr);

    // run persistence tests
    test_persistence_concurrent(os);
    
  } else {  // main test
    using key_type = unsigned long;

    // generate 2*n unique numbers in random order
    parlay::sequence<key_type> a;
    key_type max_key;

    if (use_sparse) {
      // don't use top bit since it breaks setbench version of arttree (leis_olt_art)
      //max_key = ~0ul >> 1;
      max_key = ~0ul;
      auto x = parlay::delayed_tabulate(1.2*nn,[&] (size_t i) {
			 return (key_type) parlay::hash64(i);}); // generate 64-bit keys
      //return (key_type) ((parlay::hash64(i) << 1) >> 1);}); // generate 63-bit keys
      auto xx = parlay::remove_duplicates(x);
      auto y = parlay::random_shuffle(xx);
      // don't use zero since it breaks setbench code
      a = parlay::tabulate(nn, [&] (size_t i) {return std::min(max_key,y[i]+1);});
    } else {
      max_key = nn;
      a = parlay::random_shuffle(parlay::tabulate(nn, [] (key_type i) {
					   return i+1;}));
    }
    key_type range_gap = (max_key/n)*range_size;

    parlay::sequence<key_type> b;
    if (use_zipfian) { 
      Zipfian z(nn, zipfian_param);
      b = parlay::tabulate(m, [&] (int i) { return a[z(i)]; });
    } else
      b = parlay::tabulate(m, [&] (int i) {return a[parlay::hash64(i) % nn]; });

    // parlay::parallel_for(0, m, [&] (size_t i) { assert(b[i] != 0); });
    
    // initially set to all finds
    parlay::sequence<op_type> op_types(m, Find);

    op_types = parlay::tabulate(m, [&] (size_t i) -> op_type {
        auto h = parlay::hash64(m+i)%200;
	if (h < update_percent) return Insert; 
	else if (h < 2*update_percent) return Remove;
	else if (h < 2*update_percent + 2*range_percent) return Range;
	else if (h < 2*update_percent + 2*range_percent + 2*multifind_percent)
	  return MultiFind;
	else return Find; });
    
    parlay::internal::timer t;
    if (shuffle) os.shuffle(n);
    auto tr = os.empty(buckets);

    for (int i = 0; i < rounds+1; i++) {
      long len;
      if (do_check) {
	size_t len = os.check(tr);
	if (len != 0) {
	  std::cout << "BAD LENGTH = " << len << std::endl;
	} else if(verbose) {
	  std::cout << "CHECK PASSED" << std::endl;
	}
      }
      
      if (verbose) std::cout << "round " << i << std::endl;
      if (fixed_time) {
	if (balanced_tree) {
	  auto x = parlay::sort(parlay::remove_duplicates(a.head(n)));
	  auto y = x.head(x.size());
	  insert_balanced(os, tr, y);
	} else {
	  if (initialize_with_deletes) {
	    parlay::parallel_for(0, nn, [&] (size_t i) {
					  os.insert(tr, a[i], 123); });
	    parlay::parallel_for(n, nn, [&] (size_t i) {
					  os.remove(tr, a[i]); });
	  } else {
	    parlay::parallel_for(0, n, [&] (size_t i) {
					 os.insert(tr, a[i], 123); });
	  }
	}
	//long start_timestamp = global_stamp.get_stamp();
  
        if (do_check) {
          //size_t expected = parlay::remove_duplicates(a.head(n)).size();
	  size_t expected = n;
          size_t got = os.check(tr);
          if (expected != got) {
            std::cout << "expected " << expected
                << " keys after insertion, found " << got << std::endl;
            abort();
          } else if(verbose) {
            std::cout << "CHECK PASSED" << std::endl;
          }
        }
        parlay::sequence<size_t> totals(p);
	parlay::sequence<long> addeds(p);
	parlay::sequence<long> range_counts(p);
	parlay::sequence<long> mfind_counts(p);
	parlay::sequence<long> retry_counts(p);
	parlay::sequence<long> update_counts(p);
	parlay::sequence<long> query_counts(p);
        size_t mp = m/p;
        t.start();
        auto start = std::chrono::system_clock::now();
        std::atomic<bool> finish = false;
        parlay::parallel_for(0, p, [&] (size_t i) {
             int cnt = 0;
             size_t j = i*mp;
             size_t total = 0;
	     long added = 0;
	     long range_count = 0;
	     long mfind_count = 0;
	     long retry_count = 0;
	     long update_count = 0;
	     long query_count = 0;
	     volatile long keysum = 0;
             while (true) {
               // every once in a while check if time is over
               if (cnt >= 100) { 
                 cnt = 0;
		 auto current = std::chrono::system_clock::now();
		 double duration = std::chrono::duration_cast<std::chrono::milliseconds>(current - start).count();
                 if (duration > 1000*trial_time || finish) {
                   totals[i] = total;
		   addeds[i] = added;
		   range_counts[i] = range_count;
		   mfind_counts[i] = mfind_count;
		   retry_counts[i] = retry_count;
		   update_counts[i] = update_count;
		   query_counts[i] = query_count;
                   return;
                 }
               }
               // check that no overflow
               if (j >= (i+1)*mp) abort();
	       
	       if (op_types[j] == Find) {
		 query_count++;
		 auto val = os.find(tr, b[j]);
		 if(val.has_value()) keysum += val.value();
	       }
	       else if (op_types[j] == Insert) {
		 update_count++;
		 if (os.insert(tr, b[j], 123)) added++;}
	       else if (op_types[j] == Remove) {
		 update_count++;
		 if (os.remove(tr, b[j])) added--;}
	       else if (op_types[j] == Range) {
#ifdef Range_Search
		 key_type end = ((b[j] > max_key - range_gap)
				 ? max_key : b[j] + range_gap);
		 range_count += vl::with_snapshot([&] {
				    long cnt=0;
				    auto addf = [&] (K x, V y) { cnt++;};
				    os.range_(tr, addf, b[j], end);
#ifdef LazyStamp
				    if (vl::aborted) retry_count++;
#endif
				    return cnt;});
#endif
	       } else { // multifind
		 mfind_count++;
		 keysum += vl::with_snapshot([&] {
	           long tmp_sum = 0;
		   long loc = j;
		   for (long k = 0; k < range_size; k++) {
		     auto val = os.find_(tr, b[loc]);
		     loc += 1;
		     if (loc >= (i+1)*mp) loc -= mp;
		     if(val.has_value()) tmp_sum += val.value();
#ifdef LazyStamp
		     if (vl::aborted) {
		       retry_count++;
		       return 0l;
		     }
#endif
		   }
		   return tmp_sum;});
		 j += range_size;
		 if (j >= (i+1)*mp) j -= mp;
		 cnt += range_size;
		 total += range_size;
		 continue;
	       }
	       if (++j >= (i+1)*mp) j -= mp;
               cnt++;
               total++;
             }}, 1);
        double duration = t.stop();

	if (i != 0) { // don't report zeroth round -- warmup
	  if (finish && (duration < trial_time/4))
	    std::cout << "warning out of samples, finished in "
		      << duration << " seconds" << std::endl;

	  //std::cout << duration << " : " << trial_time << std::endl;
	  size_t num_ops = parlay::reduce(totals);
	  std::cout << std::setprecision(4)
		    << P.commandName() << ","
		    << update_percent << "%update,"
		    << range_percent << "%range,"
		    << multifind_percent << "%mfind,"
		    << "rs=" << range_size << ","
		    << "n=" << n << ","
		    << "p=" << p << ","
		    << "z=" << zipfian_param << ","
		    << num_ops / (duration * 1e6) << std::endl;
    // std::cout << "timestamp increments (per microsecond): " << (global_stamp.get_stamp()-start_timestamp)*1.0/(duration * 1e6) << std::endl;
    // std::cout << "final timestamp: " << global_stamp.get_stamp() << std::endl;
	  if (do_check) {
	    size_t final_cnt = os.check(tr);
	    long updates = parlay::reduce(addeds);
	    if (multifind_percent > 0) {
	      long mfind_sum = parlay::reduce(mfind_counts);
	      long retry_sum = parlay::reduce(retry_counts);
#ifdef LazyStamp
	      std::cout << "retry percent = " << (double) 100 * retry_sum / mfind_sum
			<< std::endl;
#endif
	    }
	    if (range_percent > 0) {
	      long range_sum = parlay::reduce(range_counts);
	      long retry_sum = parlay::reduce(retry_counts);
	      long num_queries = num_ops * range_percent / 100;
	      std::cout << "average range size = " << ((float) range_sum) / num_queries
#ifdef LazyStamp
			<< ", retry percent = " << (double) 100 * retry_sum / num_queries 
#endif
			<< std::endl;
	    }

	    if (n + updates != final_cnt) {
	      std::cout << "bad size: intial size = " << n 
			<< ", added " << updates
			<< ", final size = " << final_cnt 
			<< std::endl;
	    } else if(verbose) {
              std::cout << "CHECK PASSED" << std::endl;
            }
          }
	  // if (stats) {
	  //   descriptor_pool.stats();
	  //   log_array_pool.stats();
	  //   os.stats();
	  // }
	}
        parlay::parallel_for(0, nn, [&] (size_t i) { os.remove(tr, a[i]); });
      }
      else {
        // millions of operations per second
        auto mops = [=] (double time) -> float {return m / (time * 1e6);};
	long len = parlay::remove_duplicates(b).size();
	parlay::sequence<bool> flags(m);

	
        t.start();
        parlay::parallel_for(0, m, [&] (size_t i) {
                   flags[i] = os.insert(tr, b[i], 123);
                 });

	//os.print(tr);
        std::cout << "insert," << m << "," << mops(t.stop()) << std::endl;

        if (do_check) {
	  long total = parlay::count(flags, true);
	  long lenc = os.check(tr);
	  if (lenc != len || total != len) {
	    std::cout << "incorrect size after insert: inserted="
		      << len << " succeeded=" << total << " found=" << lenc
		      << std::endl;
	    //os.print(tr);
	    auto x = parlay::sort(parlay::remove_duplicates(b));
	    //for (auto y : x)
	    //  std::cout << y << std::endl;
	  }
	}
        if (stats) {
          //descriptor_pool.stats();
          //log_array_pool.stats();
          os.stats();
        }

        auto search_seq = parlay::random_shuffle(b, parlay::random(1));
        t.start();
        parlay::parallel_for(0, m, [&] (size_t i) {
             if (!os.find(tr, search_seq[i]).has_value()) {
               std::cout << "key not found, i = " << i << ", key = " << search_seq[i] << std::endl;
               abort();
             }});
        std::cout << "find," << m << "," << mops(t.stop()) << std::endl;

        if (true) {
          auto delete_seq = parlay::random_shuffle(b, parlay::random(1));
          t.start();
          parlay::parallel_for(0, m, [&] (size_t i) {
                     os.remove(tr, delete_seq[i]);
                   });

          std::cout << "remove," << m << "," << mops(t.stop()) << std::endl;
          if (do_check) {
            len = os.check(tr);
            if (len != 0) {
              std::cout << "BAD LENGTH = " << len << std::endl;
            } else if(verbose) {
              std::cout << "CHECK PASSED" << std::endl;
            }
          }
          //if (stats) os.stats();
        }
      }
      //os.retire(tr); // free the ord_set (should be empty, but not with arttree)
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
    }
    os.retire(tr);
  }
  //if (verbose)
  //  std::cout << "final timestamp: " << vl::global_stamp.get_stamp() << std::endl;
}
