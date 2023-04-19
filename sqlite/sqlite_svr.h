#ifndef YCSB_C_SQLITE_SVR_H_
#define YCSB_C_SQLITE_SVR_H_

#include <sqlite3.h>

#include <unordered_map>

#include "rpc.h"

void read_handler(erpc::ReqHandle *req_handle, void *context);
void insert_handler(erpc::ReqHandle *req_handle, void *context);
void update_handler(erpc::ReqHandle *req_handle, void *context);
void delete_handler(erpc::ReqHandle *req_handle, void *context);

class ServerContext {
 public:
  ~ServerContext() {
    for (auto &st : prepared_stmts_) {
      sqlite3_finalize(st.second);
    }
  }

  erpc::Rpc<erpc::CTransport> *rpc_;
  erpc::MsgBuffer resp_buf_;
  static sqlite3 *db_;
  std::unordered_map<uint8_t, sqlite3_stmt *> prepared_stmts_;
  int session_num_;
  int thread_id_;
};

#endif
