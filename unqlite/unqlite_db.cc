//
//  unqlite_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2024 Richard So <rso31@gatech.edu>.
//

#include "unqlite_db.hh"
#include "core/core_workload.h"
#include "core/db.h"
#include "core/db_factory.h"
#include "unqlite/unqlite.h"
#include "utils/properties.h"
#include "utils/utils.h"
#include <cstddef>
#include <string>

#define MAX_BUFFER_LEN 8192

namespace {
const std::string PROP_DB_PATH = "unqlite.dbname";
const std::string PROP_DB_PATH_DEFAULT = "/tmp/ycsb-unqlitedb";

const std::string PROP_USEMEM = "unqlite.usemem";
const std::string PROP_USEMEM_DEFAULT = "false";
} // namespace

namespace ycsbc {

std::mutex UnqliteDB::mu_;

size_t UnqliteDB::field_count_;
std::string UnqliteDB::field_prefix_;

void UnqliteDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;

  field_count_ = std::stoi(props.GetProperty(
      CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT));
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                    CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

  // in-memory store toggle
  const bool use_mem =
      (props.GetProperty(PROP_USEMEM, PROP_USEMEM_DEFAULT) == "true");

  std::string db_path =
      use_mem ? "mem:" : props.GetProperty(PROP_DB_PATH, PROP_DB_PATH_DEFAULT);

  // init actual database
  rc = unqlite_open(&pDb, db_path.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    unqlite_lib_shutdown();
    throw utils::Exception("Unqlite open failed");
  }
}

DB::Status UnqliteDB::Read(const std::string &table, const std::string &key,
                           const std::vector<std::string> *fields,
                           std::vector<Field> &result) {
  // read the row
  char data[MAX_BUFFER_LEN];
  unqlite_int64 pBufLen = MAX_BUFFER_LEN;
  rc = unqlite_kv_fetch(pDb, key.c_str(), key.length(), data, &pBufLen);
  if (rc == UNQLITE_NOTFOUND) {
    return kNotFound;
  } else if (rc != UNQLITE_OK) {
    throw utils::Exception(std::string("Unqlite fetch failed w/ code: ") +
                           std::to_string(rc));
  }

  // deserialize the row and append to result vector
  if (fields != nullptr) {
    DeserializeRowFilter(&result, data, pBufLen, *fields);
  } else {
    DeserializeRow(&result, data, pBufLen);
  }
  return kOK;
}

DB::Status UnqliteDB::Scan(const std::string &table, const std::string &key,
                           int len, const std::vector<std::string> *fields,
                           std::vector<std::vector<Field>> &result) {
  unqlite_kv_cursor *pCur = NULL;
  // init a cursor for scanning
  rc = unqlite_kv_cursor_init(pDb, &pCur);
  if (rc != UNQLITE_OK) {
    throw utils::Exception(std::string("Unqlite cursor init failed w/ code:") +
                           std::to_string(rc));
  }

  // seek to the start key
  rc = unqlite_kv_cursor_seek(pCur, key.c_str(), key.length(),
                              UNQLITE_CURSOR_MATCH_EXACT);
  if (rc == UNQLITE_NOTFOUND) {
    return kNotFound;
  } else if (rc != UNQLITE_OK) {
    throw utils::Exception(std::string("Unqlite cursor seek failed w/ code:") +
                           std::to_string(rc));
  }

  // start scanning
  char data[MAX_BUFFER_LEN];
  unqlite_int64 pBufLen = MAX_BUFFER_LEN;
  for (int i = 0; unqlite_kv_cursor_valid_entry(pCur) && i < len; i++) {
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    // read from cursor & handle erorrs
    rc = unqlite_kv_cursor_data(pCur, data, &pBufLen);
    if (rc != UNQLITE_OK) {
      int rcc = unqlite_kv_cursor_release(pDb, pCur);
      if (rcc != UNQLITE_OK) {
        throw utils::Exception(
            std::string("Unqlite cursor release failed w/ code:") +
            std::to_string(rcc));
      }
      throw utils::Exception(
          std::string("Unqlite cursor data failed w/ code:") +
          std::to_string(rc));
    }
    // deserialize row to the Fields vector residing in `result`
    if (fields != nullptr) {
      DeserializeRowFilter(&values, data, pBufLen, *fields);
    } else {
      DeserializeRow(&values, data, pBufLen);
    }
    // seek to the next, break if invalid
    rc = unqlite_kv_cursor_next_entry(pCur);
    if (rc != UNQLITE_OK)
      break;
  }

  // release cursor
  rc = unqlite_kv_cursor_release(pDb, pCur);
  if (rc != UNQLITE_OK) {
    throw utils::Exception(
        std::string("Unqlite cursor release failed w/ code:") +
        std::to_string(rc));
  }
  return kOK;
}

DB::Status UnqliteDB::Update(const std::string &table, const std::string &key,
                             std::vector<Field> &values) {
  // search for the existing record
  char prev_data[MAX_BUFFER_LEN];
  unqlite_int64 pBufLen = MAX_BUFFER_LEN;
  rc = unqlite_kv_fetch(pDb, key.c_str(), key.length(), prev_data, &pBufLen);
  if (rc == UNQLITE_NOTFOUND) {
    throw utils::Exception("Unqlite update failed, key not found");
  } else if (rc != UNQLITE_OK) {
    throw utils::Exception("Unqlite fetch failed, code " + std::to_string(rc));
  }

  // assert fields to update do exist & update them
  std::vector<Field> prev_values;
  DeserializeRow(&prev_values, prev_data, pBufLen);
  for (Field &new_field : values) {
    bool found MAYBE_UNUSED = false;
    for (Field &prev_field : prev_values) {
      if (prev_field.name == new_field.name) {
        found = true;
        prev_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }

  // perform the store operation
  std::string data;
  SerializeRow(prev_values, &data);
  rc = unqlite_kv_store(pDb, key.c_str(), key.length(), data.c_str(),
                        data.size());
  if (rc != UNQLITE_OK) {
    throw utils::Exception("Unqlite store failed, code " + std::to_string(rc));
  }

  return kOK;
}

DB::Status UnqliteDB::Insert(const std::string &table, const std::string &key,
                             std::vector<Field> &values) {
  // perform the store operation
  std::string data;
  SerializeRow(values, &data);
  rc = unqlite_kv_store(pDb, key.c_str(), key.length(), data.c_str(),
                        data.size());
  if (rc != UNQLITE_OK) {
    throw utils::Exception("Unqlite store failed, code " + std::to_string(rc));
  }

  return kOK;
}

DB::Status UnqliteDB::Delete(const std::string &table, const std::string &key) {
  rc = unqlite_kv_delete(pDb, key.c_str(), key.length());
  if (rc != UNQLITE_OK) {
    throw utils::Exception("Unqlite delete failed, code " + std::to_string(rc));
  }
  return kOK;
}

void UnqliteDB::SerializeRow(const std::vector<Field> &values,
                             std::string *data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.name.data(), field.name.size());
    len = field.value.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.value.data(), field.value.size());
  }
}

void UnqliteDB::DeserializeRowFilter(std::vector<Field> *values,
                                     const char *data_ptr, size_t data_len,
                                     const std::vector<std::string> &fields) {
  const char *p = data_ptr;
  const char *lim = p + data_len;
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values->push_back({field, value});
      filter_iter++;
    }
  }
  assert(values->size() == fields.size());
}

void UnqliteDB::DeserializeRow(std::vector<Field> *values, const char *data_ptr,
                               size_t data_len) {
  const char *p = data_ptr;
  const char *lim = p + data_len;
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
    values->push_back({field, value});
  }
  assert(values->size() == field_count_);
}

void UnqliteDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  unqlite_close(pDb);
  unqlite_lib_shutdown();
}

DB *NewUnqliteDB() { return new UnqliteDB; }

const bool registered = DBFactory::RegisterDB("unqlite", NewUnqliteDB);

} // namespace ycsbc
