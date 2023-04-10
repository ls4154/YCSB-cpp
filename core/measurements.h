//
//  measurements.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_MEASUREMENTS_H_
#define YCSB_C_MEASUREMENTS_H_

#include "core_workload.h"
#include "properties.h"

#include <atomic>

#ifdef HDRMEASUREMENT
#include <hdr/hdr_histogram.h>
#endif

#include <yaml-cpp/yaml.h>

typedef unsigned int uint;

const std::string SKIP_SECOND_PROPERTY = "secskip";

namespace ycsbc {

class Measurements {
 public:
  Measurements(int64_t sec_skip) : sec_skip_(sec_skip), report_on_(sec_skip == 0) {}
  virtual void Report(Operation op, uint64_t latency) = 0;
  virtual std::string GetStatusMsg() = 0;
  virtual void Reset() = 0;
  virtual void Emit(YAML::Node &node) = 0;
  void Start() { start_ = std::chrono::system_clock::now(); }
 protected:
  std::chrono::time_point<std::chrono::system_clock> start_;
  int sec_skip_;
  bool report_on_;
};

class BasicMeasurements : public Measurements {
 public:
  BasicMeasurements(int64_t sec_skip = 0);
  void Report(Operation op, uint64_t latency) override;
  std::string GetStatusMsg() override;
  void Reset() override;
  void Emit(YAML::Node &node) override;
 private:
  std::atomic<uint> count_[MAXOPTYPE];
  uint last_count_[MAXOPTYPE];  // the op count at last measurement
  std::atomic<uint64_t> latency_sum_[MAXOPTYPE];
  uint64_t last_latency_sum_[MAXOPTYPE];
  std::atomic<uint64_t> latency_min_[MAXOPTYPE];
  std::atomic<uint64_t> latency_max_[MAXOPTYPE];
};

#ifdef HDRMEASUREMENT
class HdrHistogramMeasurements : public Measurements {
 public:
  HdrHistogramMeasurements(int64_t sec_skip = 0);
  void Report(Operation op, uint64_t latency) override;
  std::string GetStatusMsg() override;
  void Reset() override;
  void Emit(YAML::Node &node) override;
 private:
  hdr_histogram *histogram_[MAXOPTYPE];
};
#endif

Measurements *CreateMeasurements(utils::Properties *props);

} // ycsbc

#endif // YCSB_C_MEASUREMENTS
