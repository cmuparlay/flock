#include <string>
#include <iostream>
#include <chrono>

#include <parlay/primitives.h>
#include <parlay/random.h>
#include "zipfian.h"
#include "parse_command_line.h"

using K = unsigned long;
using V = unsigned long;
#include "unordered_map.h"

struct IntHash {
    std::size_t operator()(K const& k) const noexcept {
      return k * UINT64_C(0xbf58476d1ce4e5b9);}
};

using map_type = unordered_map<K,V,IntHash>;

double geometric_mean(const std::vector<double>& vals) {
    float product = 1;
    for (auto x : vals) product = product * x;
    return  pow(product, 1.0 / vals.size());
}

double test_loop(commandLine& C,
	       long n,   // num entries in map
	       long p,   // num threads
	       long rounds,  // num trials
	       double zipfian_param, // zipfian parameter [0:1) (0 is uniform, .99 is high skew)
	       int update_percent, // percent of operations that are either insert or delete (1/2 each)
	       double trial_time, // time to run one trial
	       bool verbose) {    // show some more info
  // total samples used
  long m = 10 * n + 1000 * p;

  // generate 2*n unique numbers in random order
  // get rid of top bit since growt seems to fail if used (must use it itself)
  auto x = parlay::delayed_tabulate(1.2* 2 * n,[&] (size_t i) {
		 return (K) (parlay::hash64(i) >> 1) ;}); 
  auto y = parlay::random_shuffle(parlay::remove_duplicates(x));
  auto a = parlay::tabulate(2 * n, [&] (size_t i) {return y[i];});
  //a = parlay::random_shuffle(parlay::tabulate(2 * n, [] (K i) { return i;}));

  // take m numbers from a in uniform or zipfian distribution
  parlay::sequence<K> b;
  if (zipfian_param != 0.0) { 
    Zipfian z(2 * n, zipfian_param);
    b = parlay::tabulate(m, [&] (int i) { return a[z(i)]; });
    a = parlay::random_shuffle(a);
  } else {
    b = parlay::tabulate(m, [&] (int i) {return a[parlay::hash64(i) % (2 * n)]; });
  }

  enum op_type : char {Find, Insert, Remove};

  // generate the operation types with update_percent updates
  // half the updates will be inserts and half removes
  auto op_types = parlay::tabulate(m, [&] (size_t i) -> op_type {
        auto h = parlay::hash64(m+i)%200;
        if (h < update_percent) return Insert; 
        else if (h < 2*update_percent) return Remove;
	else return Find; });

  std::vector<double> results;
  
  for (int i = 0; i < rounds; i++) {
    map_type map(n);

    // initialize the map with n distinct elements
    auto start_insert = std::chrono::system_clock::now();
    parlay::parallel_for(0, n, [&] (size_t i) {
       map.insert(a[i], 123); }, (p==1) ? n : 10, true);
    std::chrono::duration<double> insert_time = std::chrono::system_clock::now() - start_insert;
    double imops = n / insert_time.count() / 1e6;
	  
    long initial_size = map.size();

    // keep track of some statistics, one entry per thread
    parlay::sequence<size_t> totals(p);
    parlay::sequence<long> addeds(p);
    parlay::sequence<long> query_counts(p);
    parlay::sequence<long> query_success_counts(p);
    parlay::sequence<long> update_success_counts(p);
    size_t mp = m/p;
    auto start = std::chrono::system_clock::now();

    // start up p threads, each doing a sequence of operations
    parlay::parallel_for(0, p, [&] (size_t i) {
      int cnt = 0;
      size_t j = i*mp;
      size_t k = i*mp;
      size_t total = 0;
      long added = 0;
      long query_count = 0;
      long query_success_count = 0;
      long update_success_count = 0;
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
	    update_success_counts[i] = update_success_count;
	    return;
	  }
	}
	// do one of find, insert, or remove
	if (op_types[k] == Find) {
	  query_count++;
	  query_success_count += map.find(b[j]).has_value();
	} else if (op_types[k] == Insert) {
	  if (map.insert(b[j], 123)) {added++; update_success_count++;}
	} else { // (op_types[k] == Remove) 
	  if (map.remove(b[j])) {added--; update_success_count++;}
	}
	// wrap around if ran out of samples
	if (++j >= (i+1)*mp) j = i*mp;
	if (++k >= (i+1)*mp) k = i*mp + 1; // offset so different ops on different rounds
	cnt++;
	total++;
      }
    }, 1, true);
    auto current = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = current - start;

    size_t num_ops = parlay::reduce(totals);
    double mops = num_ops / (duration.count() * 1e6);
    results.push_back(mops);
    std::cout << C.commandName() << ","
              << update_percent << "%update,"
              << "n=" << n << ","
              << "p=" << p << ","
              << "z=" << zipfian_param << ","
	      << "insert_mops=" << (int) imops << ","
              << "mops=" << (int) mops << std::endl;

    size_t queries = parlay::reduce(query_counts);
    size_t updates = num_ops - queries;
    size_t queries_success = parlay::reduce(query_success_counts);
    size_t updates_success = parlay::reduce(update_success_counts);
    double qratio = (double) queries_success / queries;
    double uratio = (double) updates_success / updates;
    size_t final_cnt = map.size();
    long added = parlay::reduce(addeds);
    if (verbose)
      std::cout << "query success ratio = " << qratio
		<< ", update success ratio = " << uratio
		<< ", net insertions = " << added
		<< std::endl;
    if (qratio < .4 || qratio > .6)
      std::cout << "warning: query success ratio = " << qratio << std::endl;
    if (uratio < .4 || uratio > .6)
      std::cout << "warning: update success ratio = " << uratio << std::endl;
    if (initial_size + added != final_cnt) {
      std::cout << "bad size: intial size = " << initial_size
		<< ", added " << added
		<< ", final size = " << final_cnt 
		<< std::endl;
    }
  }
  return geometric_mean(results);
}
    
int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-n <size>] [-r <rounds>] [-p <procs>] [-z <zipfian_param>] [-u <update percent>]");

  long n = P.getOptionIntValue("-n", 0);
  int p = P.getOptionIntValue("-p", parlay::num_workers());  
  int rounds = P.getOptionIntValue("-r", 1);
  double zipfian_param = P.getOptionDoubleValue("-z", -1.0);
  int update_percent = P.getOptionIntValue("-u", -1);
  double trial_time = P.getOptionDoubleValue("-t", 1.0);
  bool verbose = P.getOption("-verbose");

  std::vector<long> sizes {100000, 10000000};
  std::vector<int> percents{5, 50};
  std::vector<double> zipfians{0, .99};
  if (n != 0) sizes = std::vector<long>{n};
  if (update_percent != -1) percents = std::vector<int>{update_percent};
  if (zipfian_param != -1.0) zipfians = std::vector<double>{zipfian_param};

  std::vector<double> results;
  for (auto zipfian_param : zipfians) 
    for (auto update_percent : percents) {
      for (auto n : sizes) 
	results.push_back(test_loop(P, n, p, rounds, zipfian_param, update_percent, trial_time, verbose));
	std::cout << std::endl;
    }
  std::cout << "geometric mean of mops = " << geometric_mean(results) << std::endl;
}
