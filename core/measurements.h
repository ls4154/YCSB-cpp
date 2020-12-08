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
  Measurements() : count_{}, latency_sum_{} {}
  void Report(Operation op, uint64_t latency) {
    count_[op].fetch_add(1, std::memory_order_relaxed);
    latency_sum_[op].fetch_add(latency, std::memory_order_relaxed);
  }
  uint64_t GetCount(Operation op) {
    return count_[op].load(std::memory_order_relaxed);
  }
  uint64_t GetLatency(Operation op) {
    uint64_t cnt = GetCount(op);
    return cnt > 0 ? latency_sum_[op].load(std::memory_order_relaxed) / GetCount(op) : 0;
  }
 private:
  std::atomic<uint> count_[MAXOPTYPE];
  std::atomic<uint64_t> latency_sum_[MAXOPTYPE];
};

} // ycsbc

#endif // YCSB_C_MEASUREMENTS
