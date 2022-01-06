//
//  from YCSB-cpp
//
//  Copyright (c) 2021 Guy Blelloch (adapted and simplified)
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include "parlay/primitives.h"

struct Zipfian {
public:
  static constexpr double kZipfianConst = 0.75; //99;
  static constexpr uint64_t kMaxNumItems = (UINT64_MAX >> 24);

  Zipfian(uint64_t num_items, double zipfian_const = kZipfianConst) :
  items_(num_items), theta_(zipfian_const), zeta_n_(num_items) {
    assert(items_ >= 2 && items_ < kMaxNumItems);
    zeta_2_ = Zeta(2, theta_);
    zeta_n_ = Zeta(num_items, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    eta_ = Eta();
  }

  uint64_t operator () (size_t i) {
    uint64_t r = parlay::hash64(i);
    double u = ((double) r)/std::numeric_limits<uint64_t>::max(); // uniform between 0.0 and 1.0
    double uz = u * zeta_n_;
    if (uz < 1.0) return 0;
    if (uz < 1.0 + std::pow(0.5, theta_)) return 1;
    return std::lround((items_-1) * std::pow(eta_ * u - eta_ + 1, alpha_));
  }

  double Eta() {
    return (1 - std::pow(2.0 / items_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_);
  }

  static double Zeta(uint64_t cur_num, double theta) {
    return parlay::reduce(parlay::delayed_tabulate(cur_num, [=] (size_t i) {
	  return 1.0/ std::pow(i+1, theta);}));
  }

  uint64_t items_;
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
};
