//
//  lmdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LMDB_DB_H_
#define YCSB_C_LMDB_DB_H_

#include <string>
#include <mutex>

#include "core/db.h"

#include <lmdb.h>

namespace ycsbc {

class LmdbDB : public DB {
 public:
  LmdbDB() {}
  ~LmdbDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);

 private:
  void SerializeRow(const std::vector<Field> &values, std::string *data);
  void DeserializeRowFilter(std::vector<Field> *values, const char *data_ptr, size_t data_len,
                            const std::vector<std::string> &fields);
  void DeserializeRow(std::vector<Field> *values, const char *data_ptr, size_t data_len);

  static size_t field_count_;
  static std::string field_prefix_;

  static MDB_env *env_;
  static MDB_dbi dbi_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB *NewLmdbDB();

} // ycsbc

#endif // YCSB_C_LMDB_DB_H_

