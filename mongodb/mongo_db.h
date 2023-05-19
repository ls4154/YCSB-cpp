#ifndef YCSB_C_MONGO_DB_H_
#define YCSB_C_MONGO_DB_H_

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/write_concern.hpp>

#include "core/db.h"

namespace ycsbc {

class MongoDB : public DB {
 public:
  void Init() override;
  void Cleanup() override { delete conn_; }

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
  static mongocxx::instance inst_;
  mongocxx::client *conn_;
  mongocxx::database db_;
  mongocxx::collection collection_;

 protected:
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
};
};  // namespace ycsbc

#endif
