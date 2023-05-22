#ifndef YCSB_C_MARIA_DB_H_
#define YCSB_C_MARIA_DB_H_

#include <mariadb/conncpp.hpp>

#include "core/db.h"

namespace ycsbc {

class MariaDB : public DB {
 public:
  void Init() override;
  void Cleanup() override {
    if (ins_stmt_) delete ins_stmt_;
    if (conn_) conn_->close();
  }

  Status Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
              std::vector<Field> &result) override {
    return Status::kNotImplemented;
  }
  Status Scan(const std::string &table, const std::string &key, int record_count,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return Status::kNotImplemented;
  }
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) override {
    return Status::kNotImplemented;
  }
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) override;
  Status Delete(const std::string &table, const std::string &key) override { return Status::kNotImplemented; }

 protected:
  sql::Driver *driver_;
  sql::Connection *conn_;
  sql::PreparedStatement *ins_stmt_ = nullptr;

 protected:
  bool prepareInsertStmt();

 protected:
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
};

};  // namespace ycsbc

#endif
