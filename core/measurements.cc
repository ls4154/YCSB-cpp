//
//  measurements.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "measurements.h"
#include "utils.h"

#include <limits>
#include <numeric>
#include <sstream>

namespace {
  const std::string MEASUREMENT_TYPE = "measurementtype";
#ifdef HDRMEASUREMENT
  const std::string MEASUREMENT_TYPE_DEFAULT = "hdrhistogram";
#else
  const std::string MEASUREMENT_TYPE_DEFAULT = "basic";
#endif
} // anonymous

namespace ycsbc {

BasicMeasurements::BasicMeasurements() : count_{}, latency_sum_{}, latency_max_{} {
  std::fill(std::begin(latency_min_), std::end(latency_min_), std::numeric_limits<uint64_t>::max());
}

void BasicMeasurements::Report(Operation op, uint64_t latency) {
  count_[op].fetch_add(1, std::memory_order_relaxed);
  latency_sum_[op].fetch_add(latency, std::memory_order_relaxed);
  uint64_t prev_min = latency_min_[op].load(std::memory_order_relaxed);
  while (prev_min > latency
         && !latency_min_[op].compare_exchange_weak(prev_min, latency, std::memory_order_relaxed));
  uint64_t prev_max = latency_max_[op].load(std::memory_order_relaxed);
  while (prev_max < latency
         && !latency_max_[op].compare_exchange_weak(prev_max, latency, std::memory_order_relaxed));
}

std::string BasicMeasurements::GetStatusMsg() {
  std::ostringstream msg_stream;
  msg_stream.precision(2);
  uint64_t total_cnt = 0;
  msg_stream << std::fixed << " operations;";
  for (int i = 0; i < MAXOPTYPE; i++) {
    Operation op = static_cast<Operation>(i);
    uint64_t cnt = count_[op].load(std::memory_order_relaxed);
    if (cnt == 0)
      continue;
    msg_stream << " [" << kOperationString[op] << ":"
               << " Count=" << cnt
               << " Max=" << latency_max_[op].load(std::memory_order_relaxed) / 1000.0
               << " Min=" << latency_min_[op].load(std::memory_order_relaxed) / 1000.0
               << " Avg="
               << ((cnt > 0)
                   ? static_cast<double>(latency_sum_[op].load(std::memory_order_relaxed)) / cnt
                   : 0) / 1000.0
               << "]";
    total_cnt += cnt;
  }
  return std::to_string(total_cnt) + msg_stream.str();
}

void BasicMeasurements::Reset() {
  std::fill(std::begin(count_), std::end(count_), 0);
  std::fill(std::begin(latency_sum_), std::end(latency_sum_), 0);
  std::fill(std::begin(latency_min_), std::end(latency_min_), std::numeric_limits<uint64_t>::max());
  std::fill(std::begin(latency_max_), std::end(latency_max_), 0);
}

#ifdef HDRMEASUREMENT
HdrHistogramMeasurements::HdrHistogramMeasurements() {
  for (int op = 0; op < MAXOPTYPE; op++) {
    if (hdr_init(10, 100LL * 1000 * 1000 * 1000, 3, &histogram_[op]) != 0) {
      utils::Exception("hdr init failed");
    }
  }
}

void HdrHistogramMeasurements::Report(Operation op, uint64_t latency) {
  hdr_record_value_atomic(histogram_[op], latency);
}

std::string HdrHistogramMeasurements::GetStatusMsg() {
  std::ostringstream msg_stream;
  msg_stream.precision(2);
  uint64_t total_cnt = 0;
  msg_stream << std::fixed << " operations;";
  for (int i = 0; i < MAXOPTYPE; i++) {
    Operation op = static_cast<Operation>(i);
    uint64_t cnt = histogram_[op]->total_count;
    if (cnt == 0)
      continue;
    msg_stream << " [" << kOperationString[op] << ":"
               << " Count=" << cnt
               << " Max=" << hdr_max(histogram_[op]) / 1000.0
               << " Min=" << hdr_min(histogram_[op]) / 1000.0
               << " Avg=" << hdr_mean(histogram_[op]) / 1000.0
               << " 90=" << hdr_value_at_percentile(histogram_[op], 90) / 1000.0
               << " 99=" << hdr_value_at_percentile(histogram_[op], 99) / 1000.0
               << " 99.9=" << hdr_value_at_percentile(histogram_[op], 99.9) / 1000.0
               << " 99.99=" << hdr_value_at_percentile(histogram_[op], 99.99) / 1000.0
               << "]";
    total_cnt += cnt;
  }
  return std::to_string(total_cnt) + msg_stream.str();
}

void HdrHistogramMeasurements::Reset() {
  for (int op = 0; op < MAXOPTYPE; op++) {
    hdr_reset(histogram_[op]);
  }
}
#endif

Measurements *CreateMeasurements(utils::Properties *props) {
  std::string name = props->GetProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT);

  Measurements *measurements;
  if (name == "basic") {
    measurements = new BasicMeasurements();
#ifdef HDRMEASUREMENT
  } else if (name == "hdrhistogram") {
    measurements = new HdrHistogramMeasurements();
#endif
  } else {
    measurements = nullptr;
  }

  return measurements;
}

} // ycsbc
