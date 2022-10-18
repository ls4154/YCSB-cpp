//
//  pure_insert_workload.h
//  YCSB-cpp
//
//  Copyright (c) 2022 Xuhao Luo <rossihox@gmail.com>
//

#ifndef YCSB_C_PURE_INSERT_WORKLOAD_H_
#define YCSB_C_PURE_INSERT_WORKLOAD_H_

#include "core_workload.h"

namespace ycsbc {

class PureInsertWorkload : public CoreWorkload {
 public:
  PureInsertWorkload() {}
  ~PureInsertWorkload() override {}

  void Init(const utils::Properties &p) override;
  ThreadState *InitThread(const utils::Properties &p, const int mythreadid, const int threadcount) override;

  bool DoInsert(DB &db, ThreadState *state) override;
  bool DoTransaction(DB &db, ThreadState *state) override;

 protected:
  void BuildSingleValueOfLen(std::vector<ycsbc::DB::Field> &values, const int val_len);

  int value_len;
};

class InsertThreadState : public ThreadState {
  friend class PureInsertWorkload;

 public:
  static const size_t RECORD_LENGTH;
  static const size_t KEY_LENGTH;
  static const size_t KEY_OFFSET;

 public:
  InsertThreadState(const utils::Properties &p, const int mythreadid, const int threadcount);
  ~InsertThreadState() override;

 protected:
  bool HasNextKey();
  std::string GetNextKey();

  char *workload;
  size_t len;
  off_t offset;
};

}  // namespace ycsbc

#endif  // YCSB_C_PURE_INSERT_WORKLOAD_H_
