//
//  uniform_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_RANDOM_BYTE_GENERATOR_H_
#define YCSB_C_RANDOM_BYTE_GENERATOR_H_

#include "generator.h"
#include "utils.h"

#include <random>

namespace ycsbc {

class RandomByteGenerator : public Generator<char> {
 public:
  RandomByteGenerator() : off_(6) {}

  char Next();
  char Last();

 private:
  char buf_[6];
  int off_;
};

inline char RandomByteGenerator::Next() {
  if (off_ == 6) {
    int bytes = utils::ThreadLocalRandomInt();
    buf_[0] = static_cast<char>((bytes & 31) + ' ');
    buf_[1] = static_cast<char>(((bytes >> 5) & 63) + ' ');
    buf_[2] = static_cast<char>(((bytes >> 10) & 95)+ ' ');
    buf_[3] = static_cast<char>(((bytes >> 15) & 31)+ ' ');
    buf_[4] = static_cast<char>(((bytes >> 20) & 63)+ ' ');
    buf_[5] = static_cast<char>(((bytes >> 25) & 95)+ ' ');
    off_ = 0;
  }
  return buf_[off_++];
}

inline char RandomByteGenerator::Last() {
  return buf_[(off_ - 1 + 6) % 6];
}

} // ycsbc

#endif // YCSB_C_RANDOM_BYTE_GENERATOR_H_
