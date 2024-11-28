//
//  unqlite_db.h
//  YCSB-cpp
//
//  Copyright (c) 2024 Richard So <rso31@gatech.edu>.
//

#ifndef YCSB_C_UNQLITE_DB_H_
#define YCSB_C_UNQLITE_DB_H_

#include <mutex>
#include <string>

#include "core/db.h"

#include "unqlite/unqlite.h"

namespace ycsbc {

class UnqliteDB : public DB {
public:
  UnqliteDB() {}
  ~UnqliteDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields,
              std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields,
              std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key,
                std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key,
                std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);

private:
  static std::mutex mu_;

  unqlite *pDb = NULL;

  int rc;
  unqlite_kv_cursor *pCur = NULL;

};

DB *NewUnqliteDB();

} // namespace ycsbc

#endif // YCSB_C_UNQLITE_DB_H_
