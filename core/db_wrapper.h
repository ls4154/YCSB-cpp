//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

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
  void Cleanup() {
    db_->Cleanup();
  }
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    utils::Timer<uint64_t, std::nano> timer;
    timer.Start();
    int s = db_->Read(table, key, fields, result);
    uint64_t elapsed = timer.End();
    measurements_->Report(READ, elapsed);
    return s;
  }
  int Scan(const std::string &table, const std::string &key,
           int record_count, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    utils::Timer<uint64_t, std::nano> timer;
    timer.Start();
    int s = db_->Scan(table, key, record_count, fields, result);
    uint64_t elapsed = timer.End();
    measurements_->Report(SCAN, elapsed);
    return s;
  }
  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    utils::Timer<uint64_t, std::nano> timer;
    timer.Start();
    uint64_t elapsed = timer.End();
    int s = db_->Update(table, key, values);
    measurements_->Report(UPDATE, elapsed);
    return s;
  }
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    utils::Timer<uint64_t, std::nano> timer;
    timer.Start();
    int s = db_->Insert(table, key, values);
    uint64_t elapsed = timer.End();
    measurements_->Report(INSERT, elapsed);
    return s;
  }
  int Delete(const std::string &table, const std::string &key) {
    utils::Timer<uint64_t, std::nano> timer;
    timer.Start();
    int s = db_->Delete(table, key);
    uint64_t elapsed = timer.End();
    measurements_->Report(DELETE, elapsed);
    return s;
  }
 private:
  DB *db_;
  Measurements *measurements_;
  //TODO thread local timer
  //utils::Timer<uint64_t, std::nano> timer_;
};

} // ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
