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

class Measurements {
 public:
  Measurements();
  void Report(Operation op, uint64_t latency);
  uint64_t GetCount(Operation op) {
    return count_[op].load(std::memory_order_relaxed);
  }
  double GetLatency(Operation op) {
    uint64_t cnt = GetCount(op);
    return cnt > 0
        ? static_cast<double>(latency_sum_[op].load(std::memory_order_relaxed)) / cnt
        : 0.0;
  }
  std::string GetStatusMsg();
  void Reset();
 private:
  std::atomic<uint> count_[MAXOPTYPE];
  std::atomic<uint64_t> latency_sum_[MAXOPTYPE];
  std::atomic<uint64_t> latency_min_[MAXOPTYPE];
  std::atomic<uint64_t> latency_max_[MAXOPTYPE];
};

} // ycsbc

#endif // YCSB_C_MEASUREMENTS
