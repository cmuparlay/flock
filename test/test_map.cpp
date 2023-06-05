#include <string>
#include <iostream>
#include <chrono>

#include <parlay/primitives.h>
#include <parlay/random.h>
#include "zipfian.h"
#include "parse_command_line.h"

using K = unsigned long;
using V = unsigned long;

//#include "sharded_unordered_map.h"
#include "flock_unordered_map.h"
using map_type = unordered_map<K,V>;


int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-n <size>] [-r <rounds>] [-p <procs>] [-z <zipfian_param>] [-u <update percent>]");

  enum op_type : char {Find, Insert, Remove};

  int p = P.getOptionIntValue("-p", parlay::num_workers());  
  int rounds = P.getOptionIntValue("-r", 3);
  double trial_time = P.getOptionDoubleValue("-t", 1.0);
  long n = P.getOptionIntValue("-n", 1000000);
  double zipfian_param = P.getOptionDoubleValue("-z", 0.0);
  bool use_zipfian = (zipfian_param != 0.0);
  int update_percent = P.getOptionIntValue("-u", 5); 

  // total samples used
  long m = 10 * n + 1000 * p;

  // generate 2*n unique numbers in random order
  auto x = parlay::delayed_tabulate(1.2* 2 * n,[&] (size_t i) {
	       return (K) parlay::hash64(i);}); 
  auto y = parlay::random_shuffle(parlay::remove_duplicates(x));
  auto a = parlay::tabulate(2 * n, [&] (size_t i) {return y[i];});

  // take m numbers from a in uniform or zipfian distribution
  parlay::sequence<K> b;
  if (use_zipfian) { 
    Zipfian z(2 * n, zipfian_param);
    b = parlay::tabulate(m, [&] (int i) { return a[z(i)]; });
  } else {
    b = parlay::tabulate(m, [&] (int i) {return a[parlay::hash64(i) % (2 * n)]; });
  }

  // generate the operation types with update_percent updates
  // half the updates will be inserts and half removes
  auto op_types = parlay::tabulate(m, [&] (size_t i) -> op_type {
        auto h = parlay::hash64(m+i)%200;
        if (h < update_percent) return Insert; 
        else if (h < 2*update_percent) return Remove;
	else return Find; });
    
  for (int i = 0; i < rounds; i++) {
    map_type map(n);

    // initialize the map with n distinct elements
    parlay::parallel_for(0, n, [&] (size_t i) {
	map.insert(a[i], 123); }, 10, true);

    if (map.size() != n)
      std::cout << "Error: keys not properly inserted, or size is incorrect" << std::endl;

    parlay::sequence<size_t> totals(p);
    parlay::sequence<long> addeds(p);
    parlay::sequence<long> query_counts(p);
    parlay::sequence<long> query_success_counts(p);
    size_t mp = m/p;
    auto start = std::chrono::system_clock::now();
    parlay::parallel_for(0, p, [&] (size_t i) {
      int cnt = 0;
      size_t j = i*mp;
      size_t total = 0;
      long added = 0;
      long query_count = 0;
      long query_success_count = 0;
      while (true) {
	// every once in a while check if time is over
	if (cnt >= 100) { 
	  cnt = 0;
	  auto current = std::chrono::system_clock::now();
	  std::chrono::duration<double> duration = current - start;
	  if (duration.count() > trial_time) {
	    totals[i] = total;
	    addeds[i] = added;
	    query_counts[i] = query_count;
	    query_success_counts[i] = query_success_count;
	    return;
	  }
	}
	if (op_types[j] == Find) {
	  query_count++;
	  query_success_count += map.find(b[j]).has_value();
	} else if (op_types[j] == Insert) {
	  if (map.insert(b[j], 123)) added++;
	} else if (op_types[j] == Remove) {
	  if (map.remove(b[j])) added--;
	}
	if (++j >= (i+1)*mp) j = i*mp;
	cnt++;
	total++;
      }
    }, 1);
    auto current = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = current - start;

    size_t num_ops = parlay::reduce(totals);
    std::cout << std::setprecision(4)
              << P.commandName() << ","
              << update_percent << "%update,"
              << "n=" << n << ","
              << "p=" << p << ","
              << "z=" << zipfian_param << ","
              << num_ops / (duration.count() * 1e6) << std::endl;

    size_t queries = parlay::reduce(query_counts);
    size_t queries_success = parlay::reduce(query_success_counts);
    double qratio = (double) queries_success / queries;
    //std::cout << "query success ratio = " << qratio << std::endl;
    if (qratio < .4 || qratio > .6)
      std::cout << "warning: query success ratio = " << qratio << std::endl;

    size_t final_cnt = map.size();
    long updates = parlay::reduce(addeds);
    if (n + updates != final_cnt) {
      std::cout << "bad size: intial size = " << n
		<< ", added " << updates
		<< ", final size = " << final_cnt 
		<< std::endl;
    }
    parlay::parallel_for(0, 2*n, [&] (size_t i) { map.remove(a[i]); });
  }
}
