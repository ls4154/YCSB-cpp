#include "sqlocal_db.h"

#include <iostream>
#include <mutex>
#include <string>

#include "core/db_factory.h"

#define DEBUG 0
#define ERR_DEBUG 1

namespace {
const std::string TABLE_NAME = "usertable";
const std::string PRIMARY_KEY = "YCSB_KEY";
const std::string COLUMN_NAME = "YCSB_VALUE";

const std::string PROP_NAME = "sqlocal.dbname";
const std::string PROP_NAME_DEFAULT = "";

const std::string PROP_WAL_AUTOCHECKPOINT = "sqlocal.wal_autocheckpoint";
const std::string PROP_WAL_AUTOCHECKPOINT_DEFAULT = "1000";

const std::string PROP_LOCKING_MODE = "sqlocal.locking_mode";
const std::string PROP_LOCKING_MODE_DEFAULT = "NORMAL";

const std::string PROP_JOURNAL_MODE = "sqlocal.journal_mode";
const std::string PROP_JOURNAL_MODE_DEFAULT = "WAL";

const std::string PROP_SYNCHRONOUS = "sqlocal.synchronous";
const std::string PROP_SYNCHRONOUS_DEFAULT = "NORMAL";
};  // namespace

namespace ycsbc {

using namespace std;

mutex lk;
sqlite3 *SQLocalDB::db_ = nullptr;
int SQLocalDB::ref_cnt_ = 0;

void SQLocalDB::Init() {
  lock_guard<mutex> guard(lk);
  char *err_msg;
  const utils::Properties &props = *props_;

  ref_cnt_++;
  if (db_ == nullptr) {
    int ret;

    ret = sqlite3_open(props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT).c_str(), &db_);
    if (ret != SQLITE_OK) {
      sqlite3_close(db_);
      throw utils::Exception(std::string("SQLite Open: ") + sqlite3_errmsg(db_));
    }

    int persist = 1;
    ret = sqlite3_file_control(db_, "main", SQLITE_FCNTL_PERSIST_WAL, &persist);
    if (ret != SQLITE_OK) {
      sqlite3_close(db_);
      throw utils::Exception(std::string("SQLite File Control: ") + sqlite3_errmsg(db_));
    }

    string pragmas;
    pragmas.append("PRAGMA wal_autocheckpoint=")
        .append(props.GetProperty(PROP_WAL_AUTOCHECKPOINT, PROP_WAL_AUTOCHECKPOINT_DEFAULT))
        .append(";");
    pragmas.append("PRAGMA locking_mode=")
        .append(props.GetProperty(PROP_LOCKING_MODE, PROP_LOCKING_MODE_DEFAULT))
        .append(";");
    pragmas.append("PRAGMA journal_mode=")
        .append(props.GetProperty(PROP_JOURNAL_MODE, PROP_JOURNAL_MODE_DEFAULT))
        .append(";");
    pragmas.append("PRAGMA synchronous=")
        .append(props.GetProperty(PROP_SYNCHRONOUS, PROP_SYNCHRONOUS_DEFAULT))
        .append(";");
    cout << "pragma: " << pragmas << endl;
    ret = sqlite3_exec(db_, pragmas.c_str(), nullptr, nullptr, &err_msg);
    if (ret != SQLITE_OK) {
      sqlite3_close(db_);
      throw utils::Exception(std::string("Can't set journal mode to WAL, error: ") + err_msg);
    }

    // create table
    const std::string query("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + PRIMARY_KEY +
                            " TEXT NOT NULL PRIMARY KEY, " + COLUMN_NAME + " TEXT NOT NULL);");
    ret = sqlite3_exec(db_, query.c_str(), nullptr, nullptr, &err_msg);
    if (ret != SQLITE_OK) {
      sqlite3_close(db_);
      throw utils::Exception(std::string("Can't create table, error: ") + err_msg);
    }
  }
}

void SQLocalDB::Cleanup() {
  int ret;
  for (auto &st : prepared_stmts_) {
    sqlite3_finalize(st.second);
  }

  {
    lock_guard<mutex> guard(lk);
    if (--ref_cnt_) {
      return;
    }
    ret = sqlite3_close(db_);
    if (ret != SQLITE_OK) {
      throw utils::Exception(std::string("Failed to close SQLite: ") + sqlite3_errmsg(db_));
    }
  }
}

DB::Status SQLocalDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                           std::vector<Field> &result) {
  sqlite3_stmt *stmt;
  int ret;
  Status s;

  if (prepared_stmts_.find(SQL_READ_REQ) == prepared_stmts_.end()) {
    if (!prepareReadQuery()) {
      return Status::kError;
    }
  }
  stmt = prepared_stmts_[SQL_READ_REQ];
  ret = sqlite3_bind_text(stmt, 1, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to insert query, error: " << sqlite3_errstr(ret) << std::endl;
    s = Status::kError;
    goto read_ret;
  }

  ret = sqlite3_step(stmt);
  if (ret == SQLITE_ROW) {
    const char *value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    int value_size = sqlite3_column_bytes(stmt, 0);
    DeserializeRow(result, value, value + value_size);
    s = Status::kOK;
  } else if (ret == SQLITE_DONE) {
    s = Status::kNotFound;
  } else {
#if ERR_DEBUG
    std::cerr << "Failed to get column text, error: " << sqlite3_errstr(ret) << std::endl;
#endif
    s = Status::kError;
  }

read_ret:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset read query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  return s;
}

DB::Status SQLocalDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  sqlite3_stmt *stmt;
  int ret;
  Status s;
  string data;

  if (prepared_stmts_.find(SQL_UPDATE_REQ) == prepared_stmts_.end()) {
    if (!prepareUpdateQuery()) {
      return Status::kError;
    }
  }

  SerializeRow(values, data);
  stmt = prepared_stmts_[SQL_UPDATE_REQ];
  ret = sqlite3_bind_text(stmt, 1, data.data(), data.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind value to update query, error: " << sqlite3_errstr(ret) << std::endl;
    s = Status::kError;
    goto update_ret;
  }
  ret = sqlite3_bind_text(stmt, 2, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to update query, error: " << sqlite3_errstr(ret) << std::endl;
    s = Status::kError;
    goto update_ret;
  }

  ret = sqlite3_step(stmt);
  if (ret == SQLITE_DONE) {
    s = DB::Status::kOK;
  } else {
    s = DB::Status::kError;
#if ERR_DEBUG
    std::cerr << "Failed to update, error: " << sqlite3_errstr(ret) << std::endl;
#endif
  }

update_ret:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset update query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  return s;
}

DB::Status SQLocalDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  sqlite3_stmt *stmt;
  int ret;
  Status s;
  string data;

  if (prepared_stmts_.find(SQL_INSERT_REQ) == prepared_stmts_.end()) {
    if (!prepareInsertQuery()) {
      return Status::kError;
    }
  }

  SerializeRow(values, data);
  stmt = prepared_stmts_[SQL_INSERT_REQ];
  ret = sqlite3_bind_text(stmt, 1, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to insert query, error: " << sqlite3_errstr(ret) << std::endl;
    s = DB::Status::kError;
    goto insert_ret;
  }
  ret = sqlite3_bind_text(stmt, 2, data.data(), data.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind value to insert query, error: " << sqlite3_errstr(ret) << std::endl;
    s = DB::Status::kError;
    goto insert_ret;
  }

  ret = sqlite3_step(stmt);
  if (ret == SQLITE_DONE) {
    s = DB::Status::kOK;
  } else {
    s = DB::Status::kError;
#if ERR_DEBUG
    std::cerr << "Failed to insert, error: " << sqlite3_errstr(ret) << std::endl;
#endif
  }

insert_ret:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset insert query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  return s;
}

DB::Status SQLocalDB::Delete(const std::string &table, const std::string &key) {
  sqlite3_stmt *stmt;
  int ret;
  Status s;

  if (prepared_stmts_.find(SQL_DELETE_REQ) == prepared_stmts_.end()) {
    if (!prepareDeleteQuery()) {
      return DB::Status::kError;
    }
  }

  stmt = prepared_stmts_[SQL_DELETE_REQ];
  ret = sqlite3_bind_text(stmt, 1, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to delete query, error: " << sqlite3_errstr(ret) << std::endl;
    s = DB::Status::kError;
    goto delete_ret;
  }

  ret = sqlite3_step(stmt);
  if (ret == SQLITE_DONE) {
    s = DB::Status::kOK;
  } else {
    s = DB::Status::kError;
#if ERR_DEBUG
    std::cerr << "Failed to delete, error: " << sqlite3_errstr(ret) << std::endl;
#endif
  }

delete_ret:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset delete query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  return s;
}

bool SQLocalDB::prepareReadQuery() {
  sqlite3_stmt *stmt;
  const std::string query("SELECT " + COLUMN_NAME + " FROM " + TABLE_NAME + " WHERE " + PRIMARY_KEY + " = ?;");
  int ret = sqlite3_prepare_v2(db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare read query: " << query << ". error: " << sqlite3_errmsg(db_) << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  prepared_stmts_[SQL_READ_REQ] = stmt;
  return true;
}

bool SQLocalDB::prepareInsertQuery() {
  sqlite3_stmt *stmt;
  const std::string query("INSERT INTO " + TABLE_NAME + " (" + PRIMARY_KEY + "," + COLUMN_NAME + ") VALUES (?, ?);");
  int ret = sqlite3_prepare_v2(db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare insert query: " << query << ". error: " << sqlite3_errmsg(db_) << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  prepared_stmts_[SQL_INSERT_REQ] = stmt;
  return true;
}

bool SQLocalDB::prepareUpdateQuery() {
  sqlite3_stmt *stmt;
  const std::string query("UPDATE " + TABLE_NAME + " SET " + COLUMN_NAME + " = ? WHERE " + PRIMARY_KEY + " = ?;");
  int ret = sqlite3_prepare_v2(db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare update query: " << query << ". error: " << sqlite3_errmsg(db_) << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  prepared_stmts_[SQL_UPDATE_REQ] = stmt;
  return true;
}

bool SQLocalDB::prepareDeleteQuery() {
  sqlite3_stmt *stmt;
  const std::string query("DELETE FROM " + TABLE_NAME + " WHERE " + PRIMARY_KEY + " = ?;");
  int ret = sqlite3_prepare_v2(db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare delete query: " << query << ". error: " << sqlite3_errmsg(db_) << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  prepared_stmts_[SQL_DELETE_REQ] = stmt;
  return true;
}

void SQLocalDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.first.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.first.data(), field.first.size());
    len = field.second.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.second.data(), field.second.size());
  }
}

void SQLocalDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void SQLocalDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

const bool registered = DBFactory::RegisterDB("sqlocal", []() { return dynamic_cast<DB *>(new SQLocalDB); });

};  // namespace ycsbc
