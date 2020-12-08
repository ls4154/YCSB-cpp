//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "rocksdb_db.h"
#include "core/properties.h"
#include "core/utils.h"
#include "core/core_workload.h"

#include <rocksdb/status.h>
#include <rocksdb/cache.h>
#include <rocksdb/write_batch.h>

namespace ycsbc {

RocksdbDB::~RocksdbDB() {
  delete db_;
}

void RocksdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (init_done_) {
    return;
  }
  init_done_ = true;

  const utils::Properties &props = *props_;
  const std::string &db_path = props.GetProperty("rocks_path", "");
  if (db_path == "") {
    throw utils::Exception("RocksDB db path is missing");
  }

  rocksdb::Options opt;
  opt.create_if_missing = true;
  GetOptions(props, &opt);

  rocksdb::Status s;

  if (props.GetProperty("rocks_destroy", "false") == "true") {
    s = rocksdb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB DestoryDB: ") + s.ToString());
    }
  }
  s = rocksdb::DB::Open(opt, db_path, &db_);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Open: ") + s.ToString());
  }

  const std::string &format = props.GetProperty("rocks_format", "single");
  if (format == "single") {
    format_ = kSingleEntry;
    method_read_ = &RocksdbDB::ReadSingleEntry;
    method_scan_ = &RocksdbDB::ScanSingleEntry;
    method_update_ = &RocksdbDB::UpdateSingleEntry;
    method_insert_ = &RocksdbDB::InsertSingleEntry;
    method_delete_ = &RocksdbDB::DeleteSingleEntry;
  } else {
    throw utils::Exception("unknown format");
  }

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
}

void RocksdbDB::Cleanup() {
}

void RocksdbDB::GetOptions(const utils::Properties &props, rocksdb::Options *opt) {

  if (props.GetProperty("rocks_increase_parallelism", "false") == "true") {
    opt->IncreaseParallelism();
  }
  if (props.GetProperty("rocks_optimize_level_style_compaction", "false") == "true") {
    opt->OptimizeLevelStyleCompaction();
  }
}

void RocksdbDB::SerializeRow(const std::vector<Field> &values, std::string *data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.name.data(), field.name.size());
    len = field.value.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.value.data(), field.value.size());
  }
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> *values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();

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

void RocksdbDB::DeserializeRow(std::vector<Field> *values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
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
  assert(values->size() == fieldcount_);
}

int RocksdbDB::ReadSingleEntry(const std::string &table, const std::string &key,
                               const std::vector<std::string> *fields,
                               std::vector<Field> &result) {
  std::string data;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(&result, data, *fields);
  } else {
    DeserializeRow(&result, data);
  }
  return kOK;
}

int RocksdbDB::ScanSingleEntry(const std::string &table, const std::string &key,
                               int len, const std::vector<std::string> *fields,
                               std::vector<std::vector<Field>> &result) {
  rocksdb::Iterator *db_iter = db_->NewIterator(rocksdb::ReadOptions());
  db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(&values, data, *fields);
    } else {
      DeserializeRow(&values, data);
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

int RocksdbDB::UpdateSingleEntry(const std::string &table, const std::string &key,
                                 std::vector<Field> &values) {
  std::string data;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
  }
  std::vector<Field> current_values;
  DeserializeRow(&current_values, data);
  for (Field &new_field : values) {
    bool found = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }
  rocksdb::WriteOptions wopt;

  data.clear();
  SerializeRow(current_values, &data);
  s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Put: ") + s.ToString());
  }
  return kOK;
}

int RocksdbDB::InsertSingleEntry(const std::string &table, const std::string &key,
                                 std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, &data);
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Put: ") + s.ToString());
  }
  return kOK;
}

int RocksdbDB::DeleteSingleEntry(const std::string &table, const std::string &key) {
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Delete(wopt, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Delete: ") + s.ToString());
  }
  return kOK;
}

DB *NewRocksdbDB() {
  return new RocksdbDB;
}

} // ycsbc
