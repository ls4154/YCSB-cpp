//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { }

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() { }

 protected:

  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();

  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  const std::string table = workload_.NextTable();
  const std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(table, key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      break;
    case UPDATE:
      status = TransactionUpdate();
      break;
    case INSERT:
      status = TransactionInsert();
      break;
    case SCAN:
      status = TransactionScan();
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string table = workload_.NextTable();
  const std::string key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string table = workload_.NextTable();
  const std::string key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildSingleValue(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string table = workload_.NextTable();
  const std::string key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string table = workload_.NextTable();
  const std::string key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildSingleValue(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string table = workload_.NextTable();
  uint64_t key_num;
  const std::string key = workload_.NextTransactionSequenceKey(&key_num);
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  int s = db_.Insert(table, key, values);
  workload_.AcknowledgeTransactionSequenceKey(key_num);
  return s;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
