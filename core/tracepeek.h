#ifndef YCSB_C_TRACEPEEK_H_
#define YCSB_C_TRACEPEEK_H_

#include <iostream>
#include <fstream>

#include "core_workload.h"

namespace ycsbc {

class TracePeek : public CoreWorkload {
 public:
  TracePeek() : CoreWorkload() {}
  ~TracePeek() override {}

  void Init(const utils::Properties &p) override;
  bool DoTransaction(DB &db, ThreadState *state) override;
  ThreadState *InitThread(const utils::Properties &p, const int mythreadid, const int threadcount,
                          const int num_ops) override;
};

class PeekThreadState : public ThreadState {
  friend class TracePeek;

 public:
  PeekThreadState(const int mythreadid) : trace_fs("th" + std::to_string(mythreadid) + ".keys") {
    std::cout << "opened\n";
  }
  ~PeekThreadState() override {
    trace_fs.close();
    std::cout << "closed\n";
  }

 protected:
  std::ofstream trace_fs;
};

}  // namespace ycsbc

#endif
