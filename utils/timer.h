//
//  timer.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TIMER_H_
#define YCSB_C_TIMER_H_

#include <chrono>

namespace ycsbc {

namespace utils {

template <typename R, typename P = std::ratio<1>>
class Timer {
 public:
  void Start() {
    time_ = Clock::now();
  }

  R End() {
    Duration span;
    Clock::time_point t = Clock::now();
    span = std::chrono::duration_cast<Duration>(t - time_);
    return span.count();
  }

 private:
  using Duration = std::chrono::duration<R, P>;
  using Clock = std::chrono::high_resolution_clock;

  Clock::time_point time_;
};

} // utils

} // ycsbc

#endif // YCSB_C_TIMER_H_

