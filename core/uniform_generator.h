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
#include <random>

namespace ycsbc {

class UniformGenerator : public Generator<uint64_t> {
 public:
  // Both min and max are inclusive
  UniformGenerator(uint64_t min, uint64_t max) : dist_(min, max) { Next(); }

  uint64_t Next();
  uint64_t Last();

 private:
  std::mt19937_64 generator_;
  std::uniform_int_distribution<uint64_t> dist_;
  uint64_t last_int_;
};

inline uint64_t UniformGenerator::Next() {
  return last_int_ = dist_(generator_);
}

inline uint64_t UniformGenerator::Last() {
  return last_int_;
}

} // ycsbc

#endif // YCSB_C_UNIFORM_GENERATOR_H_
