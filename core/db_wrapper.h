//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <vector>
#include <string>

#include "db.h"
#include "timer.h"
#include "measurements.h"

namespace ycsbc {

class DBWrapper : public DB {
 public:
  DBWrapper(DB *db, Measurements *measurements) : db_(db), measurements_(measurements) {}
  ~DBWrapper() {
    delete db_;
  }
  void Init() {
    db_->Init();
  }
  void Close() {
    db_->Close();
  }
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    measurements_->Report(READ);
    return db_->Read(table, key, fields, result);
  }
  int Scan(const std::string &table, const std::string &key,
           int record_count, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    measurements_->Report(SCAN);
    return db_->Scan(table, key, record_count, fields, result);
  }
  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    measurements_->Report(UPDATE);
    return db_->Update(table, key, values);
  }
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    measurements_->Report(INSERT);
    return db_->Insert(table, key, values);
  }
  int Delete(const std::string &table, const std::string &key) {
    measurements_->Report(DELETE);
    return db_->Delete(table, key);
  }
 private:
  DB *db_;
  Measurements *measurements_;
  utils::Timer<double> timer_;
};

} // ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
