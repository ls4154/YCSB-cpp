//
//  lmdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include <string.h>
#include <sys/stat.h>

#include "lmdb_db.h"
#include "core/properties.h"
#include "core/utils.h"
#include "core/core_workload.h"
#include "core/db_factory.h"

#include <lmdb.h>

namespace {
  const std::string PROP_DBPATH = "lmdb.dbpath";
  const std::string PROP_DBPATH_DEFAULT = "";

  const std::string PROP_FORMAT = "lmdb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_MAPSIZE = "lmdb.mapsize";
  const std::string PROP_MAPSIZE_DEFAULT = "-1";

  const std::string PROP_NOSYNC = "lmdb.nosync";
  const std::string PROP_NOSYNC_DEFAULT = "false";

  const std::string PROP_NOMETASYNC = "lmdb.nometasync";
  const std::string PROP_NOMETASYNC_DEFAULT = "false";

  const std::string PROP_NORDAHEAD = "lmdb.noreadahead";
  const std::string PROP_NORDAHEAD_DEFAULT = "false";

  const std::string PROP_WRITEMAP = "lmdb.writemap";
  const std::string PROP_WRITEMAP_DEFAULT = "false";
} // anonymous

namespace ycsbc {

MDB_env *LmdbDB::env_;
MDB_dbi LmdbDB::dbi_;
int LmdbDB::ref_cnt_ = 0;
std::mutex LmdbDB::mutex_;

void LmdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mutex_);

  const utils::Properties &props = *props_;
  const std::string &format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleEntry;
    method_read_ = &LmdbDB::ReadSingleEntry;
    method_scan_ = &LmdbDB::ScanSingleEntry;
    method_update_ = &LmdbDB::UpdateSingleEntry;
    method_insert_ = &LmdbDB::InsertSingleEntry;
    method_delete_ = &LmdbDB::DeleteSingleEntry;
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                    CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

  if (ref_cnt_++) {
    return;
  }

  int ret;
  int env_opt = 0;
  if (props.GetProperty(PROP_NOSYNC, PROP_NOSYNC_DEFAULT) == "true") {
    env_opt |= MDB_NOSYNC;
  }
  if (props.GetProperty(PROP_NOMETASYNC, PROP_NOMETASYNC_DEFAULT) == "true") {
    env_opt |= MDB_NOMETASYNC;
  }
  if (props.GetProperty(PROP_NORDAHEAD, PROP_NORDAHEAD_DEFAULT) == "true") {
    env_opt |= MDB_NORDAHEAD;
  }
  if (props.GetProperty(PROP_WRITEMAP, PROP_WRITEMAP_DEFAULT) == "true") {
    env_opt |= MDB_WRITEMAP;
  }
  ret = mdb_env_create(&env_);
  if  (ret) {
    throw utils::Exception(std::string("Init mdb_env_create: ") + mdb_strerror(ret));
  }
  size_t map_size = std::stoul(props.GetProperty(PROP_MAPSIZE, PROP_MAPSIZE_DEFAULT));
  if (map_size >= 0) {
    ret = mdb_env_set_mapsize(env_, map_size);
    if (ret) {
      throw utils::Exception(std::string("Init mdb_env_set_mapsize: ") + mdb_strerror(ret));
    }
  }
  const std::string &db_path = props.GetProperty(PROP_DBPATH, PROP_DBPATH_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("LMDB db path is missing");
  }
  ret = mkdir(db_path.c_str(), 0775);
  if (ret && errno != EEXIST) {
    throw utils::Exception(std::string("Init mkdir: ") + strerror(errno));
  }
  ret = mdb_env_open(env_, db_path.c_str(), env_opt, 0664);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_env_open: ") + mdb_strerror(ret));
  }

  MDB_txn *txn;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_open(txn, nullptr, 0, &dbi_);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_open: ") + mdb_strerror(ret));
  }
  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Init mdb_txn_commit: ") + mdb_strerror(ret));
  }
}

void LmdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mutex_);
  if (--ref_cnt_) {
    return;
  }
  mdb_close(env_, dbi_);
  mdb_env_close(env_);
}

void LmdbDB::SerializeRow(const std::vector<Field> &values, std::string *data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.name.data(), field.name.size());
    len = field.value.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.value.data(), field.value.size());
  }
}

void LmdbDB::DeserializeRowFilter(std::vector<Field> *values, const char *data_ptr, size_t data_len,
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

void LmdbDB::DeserializeRow(std::vector<Field> *values, const char *data_ptr, size_t data_len) {
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
  assert(values->size() == fieldcount_);
}

DB::Status LmdbDB::ReadSingleEntry(const std::string &table, const std::string &key,
                                   const std::vector<std::string> *fields,
                                   std::vector<Field> &result) {
  MDB_txn *txn;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
  if (ret) {
    throw utils::Exception(std::string("Read mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
  if (ret) {
    throw utils::Exception(std::string("Read mdb_get: ") + mdb_strerror(ret));
  }
  if (fields != nullptr) {
    DeserializeRowFilter(&result, static_cast<char *>(val_slice.mv_data), val_slice.mv_size,
                         *fields);
  } else {
    DeserializeRow(&result, static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
  }
  mdb_txn_abort(txn);
  return kOK;
}

DB::Status LmdbDB::ScanSingleEntry(const std::string &table, const std::string &key, int len,
                                   const std::vector<std::string> *fields,
                                   std::vector<std::vector<Field>> &result) {
  MDB_txn *txn;
  MDB_cursor *cursor;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Scan mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_cursor_open(txn, dbi_, &cursor);
  if (ret) {
    throw utils::Exception(std::string("Scan mdb_cursor_open: ") + mdb_strerror(ret));
  }
  ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_SET);
  assert(ret != MDB_NOTFOUND);
  if (ret) {
    throw utils::Exception(std::string("Scan mdb_cursor_get: ") + mdb_strerror(ret));
  }
  for (int i = 0; !ret && i < len; i++) {
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(&values, static_cast<char *>(val_slice.mv_data), val_slice.mv_size,
                           *fields);
    } else {
      DeserializeRow(&values, static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
    }
    ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
  }
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  return kOK;
}

DB::Status LmdbDB::UpdateSingleEntry(const std::string &table, const std::string &key,
                                     std::vector<Field> &values) {
  MDB_txn *txn;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_get: ") + mdb_strerror(ret));
  }
  std::vector<Field> current_values;
  DeserializeRow(&current_values, static_cast<char *>(val_slice.mv_data), val_slice.mv_size);
  for (Field &new_field : values) {
    bool found __attribute__((unused)) = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }

  std::string data;
  SerializeRow(current_values, &data);
  val_slice.mv_data = const_cast<char *>(data.data());
  val_slice.mv_size = data.size();
  ret = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_put: ") + mdb_strerror(ret));
  }

  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Update mdb_txn_commit: ") + mdb_strerror(ret));
  }
  return kOK;
}

DB::Status LmdbDB::InsertSingleEntry(const std::string &table, const std::string &key,
                                     std::vector<Field> &values) {
  MDB_txn *txn;
  MDB_val key_slice, val_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  std::string data;
  SerializeRow(values, &data);
  val_slice.mv_data = static_cast<void *>(const_cast<char *>(data.data()));
  val_slice.mv_size = data.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Insert mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
  if (ret) {
    throw utils::Exception(std::string("Insert mdb_put: ") + mdb_strerror(ret));
  }
  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Insert mdb_txn_commit: ") + mdb_strerror(ret));
  }
  return kOK;
}

DB::Status LmdbDB::DeleteSingleEntry(const std::string &table, const std::string &key) {
  MDB_txn *txn;
  MDB_val key_slice;

  key_slice.mv_data = static_cast<void *>(const_cast<char *>(key.data()));
  key_slice.mv_size = key.size();

  int ret;
  ret = mdb_txn_begin(env_, nullptr, 0, &txn);
  if (ret) {
    throw utils::Exception(std::string("Delete mdb_txn_begin: ") + mdb_strerror(ret));
  }
  ret = mdb_del(txn, dbi_, &key_slice, nullptr);
  if (ret) {
    throw utils::Exception(std::string("Delete mdb_del: ") + mdb_strerror(ret));
  }
  ret = mdb_txn_commit(txn);
  if (ret) {
    throw utils::Exception(std::string("Delete mdb_txn_commit: ") + mdb_strerror(ret));
  }
  return kOK;
}

DB *NewLmdbDB() {
  return new LmdbDB;
}

const bool registered = DBFactory::RegisterDB("lmdb", NewLmdbDB);

} // ycsbc
