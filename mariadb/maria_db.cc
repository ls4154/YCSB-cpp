#include "maria_db.h"

#include <iostream>
#include <string>

#include "core/db_factory.h"

using namespace std;

namespace {
const string PRIMARY_KEY = "YCSB_KEY";
const string COLUMN_NAME = "YCSB_VALUE";
const string TABLE_NAME = "usertable";
};  // namespace

namespace ycsbc {

void MariaDB::Init() {
  try {
    driver_ = sql::mariadb::get_driver_instance();

    sql::SQLString url("jdbc:mariadb://localhost:3306/ycsb");
    sql::Properties properties({{"user", "user"}, {"password", "0000"}});

    conn_ = driver_->connect(url, properties);

    sql::Statement *stmt = conn_->createStatement();
    stmt->execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" + PRIMARY_KEY + " VARCHAR(255) PRIMARY KEY, " + COLUMN_NAME + " TEXT)");
    stmt->close();
  } catch (sql::SQLException &e) {
    cerr << "Error connecting to MariaDB: " << e.what() << endl;
    conn_->close();
  }
}

DB::Status MariaDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  if (!ins_stmt_) {
    if (!prepareInsertStmt()) return Status::kError;
  }

  string data;
  SerializeRow(values, data);

  try {
    ins_stmt_->setString(1, key);
    ins_stmt_->setString(2, data);
    ins_stmt_->execute();

    ins_stmt_->clearParameters();
    return Status::kOK;
  } catch (sql::SQLException &e) {
    cerr << "Failed to execute insert statement: " << e.what() << endl;
    return Status::kError;
  }
}

bool MariaDB::prepareInsertStmt() {
  try {
    ins_stmt_ = conn_->prepareStatement("INSERT INTO " + TABLE_NAME + " (" + PRIMARY_KEY + "," + COLUMN_NAME +
                                        ") VALUES (?, ?);");
  } catch (sql::SQLException &e) {
    cerr << "Failed to prepare insert statement: " << e.what() << endl;
    return false;
  }
  return true;
}

void MariaDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.first.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.first.data(), field.first.size());
    len = field.second.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.second.data(), field.second.size());
  }
}

const bool registered = DBFactory::RegisterDB("mariadb", []() { return dynamic_cast<DB *>(new MariaDB); });

};  // namespace ycsbc
