//
// Postgre(no)SQL
//

#ifndef YCSB_C_POSTGRES_DB_H_
#define YCSB_C_POSTGRES_DB_H_

#include <libpq-fe.h>

#include "core/db.h"
#include "core/properties.h"

namespace ycsbc {

class PostgresDB : public DB {
 public:
  void Init() override;
  void Cleanup() override { PQfinish(conn_); }

  Status Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
              std::vector<Field> &result) override;

  Status Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
              std::vector<std::vector<Field>> &result) override {
    return Status::kNotImplemented;
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) override;

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) override;

  Status Delete(const std::string &table, const std::string &key) override;

 protected:
  std::string createReadStatement();
  std::string createUpdateStatement();
  std::string createInsertStatement();
  std::string createDeleteStatement();

 protected:
  /**
   * 34.20. Behavior in Threaded Programs
   * One thread restriction is that no two threads attempt to manipulate the same PGconn object at the same time. In
   * particular, you cannot issue concurrent commands from different threads through the same connection object. (If you
   * need to run concurrent commands, use multiple connections.)
   */
  PGconn *conn_;
  char escape_buf_[2048];
};

}  // namespace ycsbc

#endif
