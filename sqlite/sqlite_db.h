//
//  sqlite_db.h
//  YCSB-cpp
//
//  Copyright (c) 2023 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_SQLITE_DB_H_
#define YCSB_C_SQLITE_DB_H_

#include <mutex>
#include <unordered_map>

#include "core/db.h"

#include <sqlite3.h>

namespace ycsbc {

class SqliteDB : public DB {
 public:
  SqliteDB() {}
  ~SqliteDB() {}

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
  void OpenDB();
  void SetPragma();
  void PrepareQueries();

  static sqlite3 *db_;
  static int ref_cnt_;
  static std::mutex mu_;

  static std::string key_;
  static std::string field_prefix_;
  static size_t field_count_;
  static std::string table_name_;

  sqlite3_stmt *stmt_read_all_;
  sqlite3_stmt *stmt_scan_all_;
  sqlite3_stmt *stmt_update_all_;
  sqlite3_stmt *stmt_insert_;
  sqlite3_stmt *stmt_delete_;
  std::unordered_map<std::string, sqlite3_stmt *> stmt_read_field_;
  std::unordered_map<std::string, sqlite3_stmt *> stmt_scan_field_;
  std::unordered_map<std::string, sqlite3_stmt *> stmt_update_field_;
};

DB *NewSqliteDB();

} // ycsbc

#endif // YCSB_C_SQLITE_DB_H_
