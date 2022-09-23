#include <string>
#include <iostream>
#include <sstream>

#include <parlay/primitives.h>
#include <parlay/random.h>
#include <parlay/io.h>
#include <parlay/internal/get_time.h>
#include <parlay/internal/group_by.h>
#include <flock/defs.h>
#include "zipfian.h"
#include "parse_command_line.h"

void assert_key_exists(bool b) {
  if(!b) {
    std::cout << "key not found" << std::endl;
    abort();
  }
}

template <typename SetType, typename Tree, typename Range>
void insert_balanced(SetType& os, Tree& tr, Range A) {
  if (A.size() == 0) return;
  size_t mid = A.size()/2;
  os.insert(tr, A[mid], 123);
  parlay::par_do([&] {insert_balanced(os, tr, A.cut(0,mid));},
                 [&] {insert_balanced(os, tr, A.cut(mid+1,A.size()));});
}

template <typename SetType>
void test_sets(SetType& os, size_t default_size, commandLine P) {
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
  // by neighboring list/tree nodes
  bool shuffle = P.getOption("-shuffle");

  // verbose
  verbose = P.getOption("-v");

  // clear the memory pool between rounds
  bool clear = P.getOption("-clear");

  // only relevant to strict locks (not try locks)
  wait_before_retrying_lock = P.getOption("-wait");

  // number of samples 
  long m = P.getOptionIntValue("-m", fixed_time ? (long) (trial_time * 10000000 * std::min(p, 100)) : n);
    
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

  if (init_test) {  // trivial test inserting 4 elements and deleting one
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
    // os.print(tr);
    // std::cout << "size = " << os.check(tr) << std::endl;
    os.retire(tr);
    
  } else {  // main test
    using key_type = unsigned long;

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
      if (shuffle) os.shuffle(n);
      long len;
      auto tr = os.empty(buckets);
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
          parlay::parallel_for(0, n, [&] (size_t i) {
                os.insert(tr, a[i], 123); });
	}
  
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
        size_t mp = m/p;
        t.start();
        auto start = std::chrono::system_clock::now();
        std::atomic<bool> finish = false;
        parlay::parallel_for(0, p, [&] (size_t i) {
             int cnt = 0;
             size_t j = i*mp;
             size_t total = 0;
	     long added = 0;
             while (true) {
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
               // quit early if out of samples
               if (j == (i+1)*mp) {
                 finish = true;
                 totals[i] = total;
		 addeds[i] = added;
                 return;
                 //j = i*mp;
               }
               if (op_type[j] == 0) {if (os.insert(tr, b[j], 123)) added++;}
	       else if (op_type[j] == 1) {if (os.remove(tr, b[j])) added--;}
               else {os.find(tr, b[j]);}
               j++;
               cnt++;
               total++;
             }}, 1);
        double duration = t.stop();

        if (finish && (duration < trial_time/4))
          std::cout << "warning out of samples, finished in "
              << duration << " seconds" << std::endl;

        //std::cout << duration << " : " << trial_time << std::endl;
	std::stringstream ss;
	ss << "zipfian=" << zipfian_param;
        size_t num_ops = parlay::reduce(totals);
        std::cout << std::setprecision(4)
		  << P.commandName() << ","
		  << update_percent << "%update,"
		  << "n=" << n << ","
		  << "p=" << p << ","
		  << (!use_zipfian ? "uniform" : ss.str()) << ","
	  // << (use_locks ? "lock" : "lock_free") << ","
	  // << (try_only ? "try" : "strict") << ","
		  << num_ops / (duration * 1e6) << std::endl;
        if (do_check) {
	  size_t final_cnt = os.check(tr);
	  long updates = parlay::reduce(addeds);
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

        if (false) {
          int cnt = 2 * 100/update_percent;
          parlay::parallel_for(0, m, [&] (size_t i) {os.insert(tr, b[i], i+1); });
          t.start();
          parlay::parallel_for(0, m, [&] (size_t i) {
                     if (op_type[i] == 0) os.insert(tr, b[i], 123);
                     else if (op_type[i] == 1) os.remove(tr, b[i]);
                     else os.find(tr, b[i]);
                   });
          std::cout << P.commandName() << "," << update_percent << "%update," << m << "," << mops(t.stop()) << std::endl;
          if (do_check) os.check(tr);
          //std::cout << len << std::endl;
          if (stats) os.stats();
        }
      }
      os.retire(tr); // free the ord_set (should be empty, but not with arttree)
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
  }
}
