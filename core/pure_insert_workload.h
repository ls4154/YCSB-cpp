//
//  pure_insert_workload.h
//  YCSB-cpp
//
//  Copyright (c) 2022 Xuhao Luo <rossihox@gmail.com>
//

#ifndef YCSB_C_PURE_INSERT_WORKLOAD_H_
#define YCSB_C_PURE_INSERT_WORKLOAD_H_

#include <vector>

#include "core_workload.h"

namespace ycsbc {

class PureInsertWorkload : public CoreWorkload {
 public:
  PureInsertWorkload() {}
  ~PureInsertWorkload() override {}

  void Init(const utils::Properties &p) override;
  ThreadState *InitThread(const utils::Properties &p, const int mythreadid, const int threadcount, const int num_ops) override;

  bool DoInsert(DB &db, ThreadState *state) override;
  bool DoTransaction(DB &db, ThreadState *state) override;

 protected:
  int value_len;
};

class InsertThreadState : public ThreadState {
  friend class PureInsertWorkload;

 public:
  static const size_t RECORD_LENGTH;
  static const size_t KEY_LENGTH;
  static const size_t KEY_OFFSET;

 public:
  InsertThreadState(const utils::Properties &p, const int mythreadid, const int threadcount, const int num_ops);
  ~InsertThreadState() override;

 protected:
  bool HasNextKey();
  std::string GetNextKey();

  std::vector<char *> workloads;
  char *cur_workload;
  int workload_i;
  size_t len;
  off_t offset;
};

}  // namespace ycsbc

#endif  // YCSB_C_PURE_INSERT_WORKLOAD_H_
