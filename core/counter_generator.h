//
//  counter_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_COUNTER_GENERATOR_H_
#define YCSB_C_COUNTER_GENERATOR_H_

#include "generator.h"

#include <atomic>

namespace ycsbc {

class CounterGenerator : public Generator<uint64_t> {
 public:
  CounterGenerator(uint64_t start) : counter_(start) { }
  uint64_t Next() { return counter_.fetch_add(1); }
  uint64_t Last() { return counter_.load() - 1; }
 private:
  std::atomic<uint64_t> counter_;
};

} // ycsbc

#endif // YCSB_C_COUNTER_GENERATOR_H_
