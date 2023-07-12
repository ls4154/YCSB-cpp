//
//  rate_limit.h
//  YCSB-cpp
//
//  Copyright (c) 2023 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_RATE_LIMIT_H_
#define YCSB_C_RATE_LIMIT_H_

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <ratio>
#include <thread>

namespace ycsbc {

namespace utils {

// Token bucket rate limiter for single client
class RateLimiter {
 public:
  RateLimiter(int64_t r, int64_t b) : r_(r * TOKEN_PRECISION), b_(b * TOKEN_PRECISION), tokens_(0), last_(Clock::now()) {}

  inline void Consume(int64_t n) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (r_ <= 0) {
      return;
    }

    // refill tokens
    auto now = Clock::now();
    auto diff = std::chrono::duration_cast<Duration>(now - last_);
    tokens_ = std::min(b_, tokens_ + diff.count() * r_ / 1000000000);
    last_ = now;

    // check tokens
    tokens_ -= n * TOKEN_PRECISION;

    // sleep
    if (tokens_ < 0) {
      lock.unlock();
      int64_t wait_time = -tokens_ * 1000000000 / r_;
      std::this_thread::sleep_for(std::chrono::nanoseconds(wait_time));
    }
  }

  inline void SetRate(int64_t r) {
    std::lock_guard<std::mutex> lock(mutex_);

    // refill tokens
    auto now = Clock::now();
    auto diff = std::chrono::duration_cast<Duration>(now - last_);
    tokens_ = std::min(b_, tokens_ + diff.count() * r_ * TOKEN_PRECISION / 1000000000);
    last_ = now;

    // set rate
    r_ = r * TOKEN_PRECISION;
  }

 private:
  using Clock = std::chrono::steady_clock;
  using Duration = std::chrono::nanoseconds;
  static constexpr int64_t TOKEN_PRECISION = 10000;

  std::mutex mutex_;
  int64_t r_;
  int64_t b_;
  int64_t tokens_;
  Clock::time_point last_;
};

} // utils

} // ycsbc

#endif // YCSB_C_RATE_LIMIT_H_
