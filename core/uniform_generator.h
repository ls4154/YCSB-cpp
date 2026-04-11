//
//  uniform_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UNIFORM_GENERATOR_H_
#define YCSB_C_UNIFORM_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <cstdint>
#include <random>

namespace ycsbc {

class UniformGenerator : public Generator<uint64_t> {
 public:
  // Both min and max are inclusive
  UniformGenerator(uint64_t min, uint64_t max) : min_(min), max_(max), last_int_(min) { Next(); }

  uint64_t Next();
  uint64_t Last() const override;

 private:
  const uint64_t min_;
  const uint64_t max_;
  std::atomic<uint64_t> last_int_;
};

inline uint64_t UniformGenerator::Next() {
  static thread_local std::mt19937_64 generator(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(min_, max_);
  uint64_t next = dist(generator);
  last_int_.store(next, std::memory_order_relaxed);
  return next;
}

inline uint64_t UniformGenerator::Last() const {
  return last_int_.load(std::memory_order_relaxed);
}

} // ycsbc

#endif // YCSB_C_UNIFORM_GENERATOR_H_
