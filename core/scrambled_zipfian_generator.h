//
//  scrambled_zipfian_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_
#define YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_

#include "generator.h"

#include <cstdint>
#include "utils.h"
#include "zipfian_generator.h"

namespace ycsbc {

class ScrambledZipfianGenerator : public Generator<uint64_t> {
 public:
  ScrambledZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const) :
      base_(min), num_items_(max - min + 1), generator_(0, 10000000000LL, zipfian_const) { }

  ScrambledZipfianGenerator(uint64_t min, uint64_t max) :
      base_(min), num_items_(max - min + 1),
      generator_(0, 10000000000LL, ZipfianGenerator::kZipfianConst, kZetan) { }

  ScrambledZipfianGenerator(uint64_t num_items) :
      ScrambledZipfianGenerator(0, num_items - 1) { }

  uint64_t Next();
  uint64_t Last();

 private:
  static constexpr double kZetan = 26.46902820178302;
  const uint64_t base_;
  const uint64_t num_items_;
  ZipfianGenerator generator_;

  uint64_t Scramble(uint64_t value) const;
};

inline uint64_t ScrambledZipfianGenerator::Scramble(uint64_t value) const {
  return base_ + utils::FNVHash64(value) % num_items_;
}

inline uint64_t ScrambledZipfianGenerator::Next() {
  return Scramble(generator_.Next());
}

inline uint64_t ScrambledZipfianGenerator::Last() {
  return Scramble(generator_.Last());
}

}

#endif // YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_
