//
//  acknowledged_counter_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_ACKNOWLEDGED_COUNTER_GENERATOR_H_
#define YCSB_C_ACKNOWLEDGED_COUNTER_GENERATOR_H_

#include "counter_generator.h"

#include <atomic>
#include <vector>
#include <mutex>

namespace ycsbc {

class AcknowledgedCounterGenerator : public CounterGenerator {
 public:
  AcknowledgedCounterGenerator(uint64_t start)
      : CounterGenerator(start), limit_(start - 1), ack_window_(kWindowSize, false) {}
  uint64_t Last() { return limit_.load(); }
  void Acknowledge(uint64_t value);
 private:
  static const size_t kWindowSize = (1 << 16);
  static const size_t kWindowMask = kWindowSize - 1;
  std::atomic<uint64_t> limit_;
  std::vector<bool> ack_window_;
  std::mutex mutex_;
};

} // ycsbc

#endif // YCSB_C_ACKNOWLEDGED_COUNTER_GENERATOR_H_
