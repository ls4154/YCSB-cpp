//
//  measurements.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_MEASUREMENTS_H_
#define YCSB_C_MEASUREMENTS_H_

#include "core_workload.h"
#include <atomic>

namespace ycsbc {

//TODO latency
class Measurements {
 public:
  Measurements() : count_{} {}
  void Report(Operation op, double latency) {
    count_[op].fetch_add(1, std::memory_order_relaxed);
  }
  int GetCount(Operation op) {
    return count_[op].load(std::memory_order_relaxed);
  }
 private:
  std::atomic<int> count_[MAXOPTYPE];
};

} // ycsbc

#endif // YCSB_C_MEASUREMENTS
