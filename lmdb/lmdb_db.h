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
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:
  enum LmdbFormat {
    kSingleEntry,
    kRowMajor,
    kColumnMajor
  };
  LmdbFormat format_;

  void SerializeRow(const std::vector<Field> &values, std::string *data);
  void DeserializeRowFilter(std::vector<Field> *values, const char *data_ptr, size_t data_len,
                            const std::vector<std::string> &fields);
  void DeserializeRow(std::vector<Field> *values, const char *data_ptr, size_t data_len);

  Status ReadSingleEntry(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanSingleEntry(const std::string &table, const std::string &key, int len,
                         const std::vector<std::string> *fields,
                         std::vector<std::vector<Field>> &result);
  Status UpdateSingleEntry(const std::string &table, const std::string &key,
                           std::vector<Field> &values);
  Status InsertSingleEntry(const std::string &table, const std::string &key,
                           std::vector<Field> &values);
  Status DeleteSingleEntry(const std::string &table, const std::string &key);

  Status (LmdbDB::*method_read_)(const std::string &, const std:: string &,
                                 const std::vector<std::string> *, std::vector<Field> &);
  Status (LmdbDB::*method_scan_)(const std::string &, const std::string &, int,
                                 const std::vector<std::string> *,
                                 std::vector<std::vector<Field>> &);
  Status (LmdbDB::*method_update_)(const std::string &, const std::string &, std::vector<Field> &);
  Status (LmdbDB::*method_insert_)(const std::string &, const std::string &, std::vector<Field> &);
  Status (LmdbDB::*method_delete_)(const std::string &, const std::string &);

  unsigned fieldcount_;
  std::string field_prefix_;

  static MDB_env *env_;
  static MDB_dbi dbi_;
  static int ref_cnt_;
  static std::mutex mutex_;
};

DB *NewLmdbDB();

} // ycsbc

#endif // YCSB_C_LMDB_DB_H_

