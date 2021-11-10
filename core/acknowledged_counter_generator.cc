//
//  acknowledged_counter_generator.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "acknowledged_counter_generator.h"
#include "utils.h"

namespace ycsbc {
void AcknowledgedCounterGenerator::Acknowledge(uint64_t value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t cur_slot = value & kWindowMask;
  if (ack_window_[cur_slot]) {
    throw utils::Exception("Not enough window size");
  }
  ack_window_[cur_slot] = true;
  uint64_t limit = limit_.load();
  size_t until = limit + kWindowSize;
  size_t i;
  for (i = limit + 1; i < until; i++) {
    size_t slot = i & kWindowMask;
    if (!ack_window_[slot]) {
      break;
    }
    ack_window_[slot] = false;
  }
  limit_.store(i - 1);
}

} // ycsbc

