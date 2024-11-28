//
//  unqlite_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2024 Richard So <rso31@gatech.edu>.
//

#include "unqlite_db.hh"
#include "core/db.h"
#include "core/db_factory.h"
#include "unqlite/unqlite/src/unqlite.h"
#include "unqlite/unqlite/unqlite.h"
#include "utils/utils.h"

namespace {
const std::string PROP_NAME = "unqlite.dbname";
const std::string PROP_NAME_DEFAULT = "";
} // namespace

namespace ycsbc {

void UnqliteDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  // init actual database
  rc = unqlite_open(&pDb, "mem:", UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    unqlite_lib_shutdown();
    throw utils::Exception("Unqlite open failed");
  }

  // init a cursor for seeking and scanning
  rc = unqlite_kv_cursor_init(pDb, &pCur);
  if (rc != UNQLITE_OK) {
    unqlite_lib_shutdown();
    throw utils::Exception("Unqlite cursor init failed");
  }
}

DB::Status UnqliteDB::Read(const std::string &table, const std::string &key,
                           const std::vector<std::string> *fields,
                           std::vector<Field> &result) {
  return kNotImplemented;
}

DB::Status UnqliteDB::Scan(const std::string &table, const std::string &key,
                           int len, const std::vector<std::string> *fields,
                           std::vector<std::vector<Field>> &result) {
  return kNotImplemented;
}

DB::Status UnqliteDB::Update(const std::string &table, const std::string &key,
                             std::vector<Field> &values) {
  return kNotImplemented;
}

DB::Status UnqliteDB::Insert(const std::string &table, const std::string &key,
                             std::vector<Field> &values) {
  return kNotImplemented;
}

DB::Status UnqliteDB::Delete(const std::string &table, const std::string &key) {
  return kNotImplemented;
}

void UnqliteDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  unqlite_kv_cursor_release(pDb, pCur);
  unqlite_close(pDb);
  unqlite_lib_shutdown();
}

DB *NewUnqliteDB() { return new UnqliteDB; }

const bool registered = DBFactory::RegisterDB("unqlite", NewUnqliteDB);

} // namespace ycsbc
