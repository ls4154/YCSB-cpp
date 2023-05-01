#ifndef YCSB_C_SQLOCAL_H_
#define YCSB_C_SQLOCAL_H_

#include <sqlite3.h>

#include <unordered_map>

#include "core/db.h"

namespace ycsbc {

const uint8_t SQL_READ_REQ    = 0;
const uint8_t SQL_UPDATE_REQ  = 1;
const uint8_t SQL_INSERT_REQ  = 2;
const uint8_t SQL_DELETE_REQ  = 3;
const uint8_t SQL_SCAN_REQ    = 4;

class SQLocalDB : public DB {
 public:
  SQLocalDB() {}
  ~SQLocalDB() override{};
  void Init() override;
  void Cleanup() override;

  Status Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
              std::vector<Field> &result) override;
  Status Scan(const std::string &table, const std::string &key, int record_count,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return Status::kNotImplemented;
  }
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) override;
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) override;
  Status Delete(const std::string &table, const std::string &key) override;

 protected:
  bool prepareReadQuery();
  bool prepareInsertQuery();
  bool prepareUpdateQuery();
  bool prepareDeleteQuery();

 protected:
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

 protected:
  static sqlite3 *db_;
  static int ref_cnt_;
  std::unordered_map<uint8_t, sqlite3_stmt *> prepared_stmts_;
};

};  // namespace ycsbc

#endif
