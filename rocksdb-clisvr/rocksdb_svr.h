#ifndef YCSB_C_ROCKSDB_SVR_H_
#define YCSB_C_ROCKSDB_SVR_H_

#include "common.h"
#include "rpc.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/utilities/options_util.h>

#include <mutex>

void read_handler(erpc::ReqHandle *req_handle, void *context);
void insert_handler(erpc::ReqHandle *req_handle, void *context);
void delete_handler(erpc::ReqHandle *req_handle, void *context);

class ServerContext {
 public:
  erpc::Rpc<erpc::CTransport> *rpc_;
  static rocksdb::DB *db_;
  int session_num_;
  int thread_id_;
};

#endif
