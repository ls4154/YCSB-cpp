//
//  sqlite_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2023 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "query_builder.h"
#include "core/db_factory.h"
#include "utils/properties.h"
#include "utils/utils.h"

#include "sqlite_db.h"

namespace {

const std::string PROP_DBPATH = "sqlite.dbpath";
const std::string PROP_DBPATH_DEFAULT = "";

const std::string PROP_CACHE_SIZE = "sqlite.cache_size";
const std::string PROP_CACHE_SIZE_DEFAULT = "-2000";

const std::string PROP_PAGE_SIZE = "sqlite.page_size";
const std::string PROP_PAGE_SIZE_DEFAULT = "4096";

const std::string PROP_JOURNAL_MODE = "sqlite.journal_mode";
const std::string PROP_JOURNAL_MODE_DEFAULT = "WAL";

const std::string PROP_SYNCHRONOUS = "sqlite.synchronous";
const std::string PROP_SYNCHRONOUS_DEFAULT = "NORMAL";

const std::string PROP_PRIMARY_KEY = "sqlite.primary_key";
const std::string PROP_PRIMARY_KEY_DEFAULT = "user_id";

const std::string PROP_CREATE_TABLE = "sqlite.create_table";
const std::string PROP_CREATE_TABLE_DEFAULT = "true";

static sqlite3_stmt *SQLite3Prepare(sqlite3 *db, std::string query) {
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db, query.c_str(), query.size()+1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw ycsbc::utils::Exception(std::string("prepare: ") + sqlite3_errmsg(db));
  }
  return stmt;
}

} // anonymous

namespace ycsbc {

sqlite3 *SqliteDB::db_ = nullptr;
int SqliteDB::ref_cnt_ = 0;
std::mutex SqliteDB::mu_;

std::string SqliteDB::key_;
std::string SqliteDB::field_prefix_;
size_t SqliteDB::field_count_;
std::string SqliteDB::table_name_;

void SqliteDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  // global init
  if (ref_cnt_++ == 0) {
    OpenDB();
    SetPragma();
  }

  // per-thread init
  PrepareQueries();
}

void SqliteDB::OpenDB() {
  const std::string &db_path = props_->GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("SQLite db path is missing");
  }

  int rc = sqlite3_open_v2(db_path.c_str(), &db_, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init open: ") + sqlite3_errmsg(db_));
  }

  key_ = props_->GetProperty(PROP_PRIMARY_KEY, PROP_PRIMARY_KEY_DEFAULT);
  field_prefix_ = props_->GetProperty(CoreWorkload::FIELD_NAME_PREFIX, CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);
  field_count_ = std::stoi(props_->GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT));
  table_name_ = props_->GetProperty(CoreWorkload::TABLENAME_DEFAULT, CoreWorkload::TABLENAME_DEFAULT);

  if (props_->GetProperty(PROP_CREATE_TABLE, PROP_CREATE_TABLE_DEFAULT) == "true") {
    std::vector<std::string> fields;
    fields.reserve(field_count_);
    for (size_t i = 0; i < field_count_; i++) {
        fields.push_back(field_prefix_ + std::to_string(i));
    }
    rc = sqlite3_exec(db_, BuildCreateTableQuery(table_name_, key_, fields).c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw utils::Exception(std::string("Create table: ") + sqlite3_errmsg(db_));
    }
  }
}

void SqliteDB::SetPragma() {
  int cache_size = std::stoi(props_->GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
  std::string stmt = std::string("PRAGMA cache_size = ") + std::to_string(cache_size);
  int rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec cache_size: ") + sqlite3_errmsg(db_));
  }

  int page_size = std::stoi(props_->GetProperty(PROP_PAGE_SIZE, PROP_PAGE_SIZE_DEFAULT));
  stmt = std::string("PRAGMA page_size = ") + std::to_string(page_size);
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec page_size: ") + sqlite3_errmsg(db_));
  }

  std::string journal_mode = props_->GetProperty(PROP_JOURNAL_MODE, PROP_JOURNAL_MODE_DEFAULT);
  stmt = std::string("PRAGMA journal_mode = ") + journal_mode;
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec journal_mode: ") + sqlite3_errmsg(db_));
  }

  std::string synchronous = props_->GetProperty(PROP_SYNCHRONOUS, PROP_SYNCHRONOUS_DEFAULT);
  stmt = std::string("PRAGMA synchronous = ") + synchronous;
  rc = sqlite3_exec(db_, stmt.c_str(), nullptr, nullptr, nullptr);
  if (rc != SQLITE_OK) {
    throw utils::Exception(std::string("Init exec synchronous: ") + sqlite3_errmsg(db_));
  }
}

void SqliteDB::PrepareQueries() {
  std::vector<std::string> fields;
  fields.reserve(field_count_);
  for (size_t i = 0; i < field_count_; i++) {
      fields.push_back(field_prefix_ + std::to_string(i));
  }

  // Read
  stmt_read_all_ = SQLite3Prepare(db_, BuildReadQuery(table_name_, key_, fields));
  for (size_t i = 0; i < field_count_; i++) {
    std::string field_name = field_prefix_ + std::to_string(i);
    stmt_read_field_[field_name] = SQLite3Prepare(db_, BuildReadQuery(table_name_, key_, {field_name}));
  }

  // Scan
  stmt_scan_all_ = SQLite3Prepare(db_, BuildScanQuery(table_name_, key_, fields));
  for (size_t i = 0; i < field_count_; i++) {
    std::string field_name = field_prefix_ + std::to_string(i);
    stmt_scan_field_[field_name] = SQLite3Prepare(db_, BuildScanQuery(table_name_, key_, {field_name}));
  }

  // Update
  stmt_update_all_ = SQLite3Prepare(db_, BuildUpdateQuery(table_name_, key_, fields));
  for (size_t i = 0; i < field_count_; i++) {
    std::string field_name = field_prefix_ + std::to_string(i);
    stmt_update_field_[field_name] = SQLite3Prepare(db_, BuildUpdateQuery(table_name_, key_, {field_name}));
  }

  // Insert
  stmt_insert_ = SQLite3Prepare(db_, BuildInsertQuery(table_name_, key_, fields));

  // Delete
  stmt_delete_ = SQLite3Prepare(db_, BuildDeleteQuery(table_name_, key_));
}

void SqliteDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);

  sqlite3_finalize(stmt_read_all_);
  for (auto s : stmt_read_field_) {
    sqlite3_finalize(s.second);
  }
  sqlite3_finalize(stmt_scan_all_);
  for (auto s : stmt_scan_field_) {
    sqlite3_finalize(s.second);
  }
  sqlite3_finalize(stmt_update_all_);
  for (auto s : stmt_update_field_) {
    sqlite3_finalize(s.second);
  }
  sqlite3_finalize(stmt_insert_);
  sqlite3_finalize(stmt_delete_);

  if (--ref_cnt_ == 0) {
    int rc = sqlite3_close(db_);
    assert(rc == SQLITE_OK);
  }
}

DB::Status SqliteDB::Read(const std::string &table, const std::string &key,
                          const std::vector<std::string> *fields, std::vector<Field> &result) {
  DB::Status s = kOK;
  bool temp = false;
  sqlite3_stmt *stmt;
  size_t field_cnt;

  if (fields == nullptr || fields->size() == field_count_) {
    field_cnt = field_count_;
    stmt = stmt_read_all_;
  } else if (fields->size() == 1) {
    field_cnt = 1;
    stmt = stmt_read_field_[(*fields)[0]];
  } else {
    temp = true;
    field_cnt = fields->size();;
    stmt = SQLite3Prepare(db_, BuildReadQuery(table_name_, key_, *fields));
  }

  int rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_ROW) {
    s = kNotFound;
    goto cleanup;
  }

  result.reserve(field_cnt);
  for (size_t i = 0; i < field_cnt; i++) {
    const char *name = reinterpret_cast<const char *>(sqlite3_column_name(stmt, i));
    const char *value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, i));
    result.push_back({name, value});
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
  if (temp) {
    sqlite3_finalize(stmt);
  }

  return s;
}

DB::Status SqliteDB::Scan(const std::string &table, const std::string &key, int len,
                          const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  DB::Status s = kOK;
  bool temp = false;
  sqlite3_stmt *stmt;
  size_t field_cnt;

  if (fields == nullptr || fields->size() == field_count_) {
    field_cnt = field_count_;
    stmt = stmt_scan_all_;
  } else if (fields->size() == 1) {
    field_cnt = 1;
    stmt = stmt_scan_field_[(*fields)[0]];
  } else {
    temp = true;
    field_cnt = fields->size();;
    stmt = SQLite3Prepare(db_, BuildScanQuery(table_name_, key_, *fields));
  }

  int rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }
  rc = sqlite3_bind_int(stmt, 2, len);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }

  for (int i = 0; i < len; i++) {
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
      break;
    }
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    values.reserve(field_cnt);
    // const char *user_id = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    for (size_t i = 0; i < field_cnt; i++) {
      const char *name = reinterpret_cast<const char *>(sqlite3_column_name(stmt, 1+i));
      const char *value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1+i));
      values.push_back({name, value});
    }
  }

  if (result.size() == 0) {
    s = kNotFound;
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
  if (temp) {
    sqlite3_finalize(stmt);
  }

  return s;
}

DB::Status SqliteDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  DB::Status s = kOK;
  bool temp = false;
  sqlite3_stmt *stmt;
  size_t field_cnt;

  if (values.size() == field_count_) {
    field_cnt = field_count_;
    stmt = stmt_update_all_;
  } else if (values.size() == 1) {
    field_cnt = 1;
    stmt = stmt_update_field_[values[0].name];
  } else {
    temp = true;
    std::vector<std::string> fields;
    fields.reserve(values.size());
    for (auto &f : values) {
      fields.push_back(f.name);
    }
    field_cnt = values.size();
    stmt = SQLite3Prepare(db_, BuildUpdateQuery(table_name_, key_, fields));
  }

  int rc;
  for (size_t i = 0; i < field_cnt; i++) {
    rc = sqlite3_bind_text(stmt, 1+i, values[i].value.c_str(), values[i].value.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
      s = kError;
      goto cleanup;
    }
  }

  rc = sqlite3_bind_text(stmt, 1+field_cnt, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_DONE) {
    s = kError;
    goto cleanup;
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
  if (temp) {
    sqlite3_finalize(stmt);
  }

  return s;
}


DB::Status SqliteDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  DB::Status s = kOK;
  sqlite3_stmt *stmt = stmt_insert_;

  if (field_count_ != values.size()) {
    return kError;
  }

  int rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }
  for (size_t i = 0; i < field_count_; i++) {
    rc = sqlite3_bind_text(stmt, 2+i, values[i].value.c_str(), values[i].value.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
      s = kError;
      goto cleanup;
    }
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_DONE) {
    s = kError;
    goto cleanup;
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);

  return s;
}

DB::Status SqliteDB::Delete(const std::string &table, const std::string &key) {
  DB::Status s = kOK;
  sqlite3_stmt *stmt = stmt_delete_;

  int rc = sqlite3_bind_text(stmt, 1, key.c_str(), key.size(), SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    s = kError;
    goto cleanup;
  }

  rc = sqlite3_step(stmt);
  if (rc != SQLITE_DONE) {
    s = kError;
    goto cleanup;
  }

cleanup:
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);

  return s;
}

DB *NewSqliteDB() {
  return new SqliteDB;
}

const bool registered = DBFactory::RegisterDB("sqlite", NewSqliteDB);

} // ycsbc
