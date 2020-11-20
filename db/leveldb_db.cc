//
//  leveldb_db.cc
//  YCSB-C
//
//  Created by YJ Lee.
//

#include "leveldb_db.h"
#include "core/utils.h"

#include "leveldb/options.h"
#include "leveldb/write_batch.h"

namespace ycsbc {

LeveldbDB::LeveldbDB(const utils::Properties &props) {
  const std::string &db_path = props.GetProperty("ldb_path", "");
  if (db_path == "") {
    throw utils::Exception("LevelDB db path is missing");
  }

  leveldb::Options opt;
  opt.create_if_missing = true;
  GetOptions(props, &opt);

  leveldb::Status s;

  if (props.GetProperty("ldb_destroy", "false") == "true") {
    s = leveldb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("LevelDB DestoryDB: ") + s.ToString());
    }
  }
  s = leveldb::DB::Open(opt, db_path, &db_);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Open: ") + s.ToString());
  }

  const std::string &format = props.GetProperty("ldb_format", "single");
  if (format == "single") {
    format_ = kSingleEntry;
    method_read_ = &LeveldbDB::ReadSingleEntry;
    method_scan_ = &LeveldbDB::ScanSingleEntry;
    method_update_ = &LeveldbDB::UpdateSingleEntry;
    method_insert_ = &LeveldbDB::InsertSingleEntry;
    method_delete_ = &LeveldbDB::DeleteSingleEntry;
  } else if (format == "row") {
    format_ = kRowMajor;
    method_read_ = &LeveldbDB::ReadCompKeyRM;
    method_scan_ = &LeveldbDB::ScanCompKeyRM;
    method_update_ = &LeveldbDB::InsertCompKey;
    method_insert_ = &LeveldbDB::InsertCompKey;
    method_delete_ = &LeveldbDB::DeleteCompKey;
  } else if (format == "column") {
    format_ = kColumnMajor;
    method_read_ = &LeveldbDB::ReadCompKeyCM;
    method_scan_ = &LeveldbDB::ScanCompKeyCM;
    method_update_ = &LeveldbDB::InsertCompKey;
    method_insert_ = &LeveldbDB::InsertCompKey;
    method_delete_ = &LeveldbDB::DeleteCompKey;
  } else {
    throw utils::Exception("unknown format");
  }

  fieldcount_ = 10;
  // TODO fieldcount_ = std::stoi(props["fieldcount"]);
}

LeveldbDB::~LeveldbDB() {
  delete db_;
}

void LeveldbDB::GetOptions(const utils::Properties &props, leveldb::Options *opt) {
  size_t writer_buffer_size = std::stol(props.GetProperty("ldb_write_buffer_size", "0"));
  if (writer_buffer_size > 0) {
    opt->write_buffer_size = writer_buffer_size;
  }
  props.GetProperty("ldb_max_file_size", "0");
  size_t max_file_size = std::stol(props.GetProperty("ldb_max_file_size", "0"));
  if (max_file_size > 0) {
    opt->max_file_size = max_file_size;
  }
  size_t cache_size = std::stol(props.GetProperty("ldb_cache_size", "0"));
  if (cache_size > 0) {
    opt->block_cache = leveldb::NewLRUCache(cache_size);
  }
  int max_open_files = std::stoi(props.GetProperty("ldb_max_open_files", "0"));
  if (max_open_files > 0) {
    opt->max_open_files = max_open_files;
  }
  std::string compression = props.GetProperty("ldb_compression", "no");
  if (compression == "snappy") {
    opt->compression = leveldb::kSnappyCompression;
  } else {
    opt->compression = leveldb::kNoCompression;
  }
  int filter_bits = std::stoi(props.GetProperty("ldb_filter_bits", "0"));
  if (filter_bits > 0) {
    opt->filter_policy = leveldb::NewBloomFilterPolicy(filter_bits);
  }
}

void LeveldbDB::SerializeRow(const std::vector<KVPair> &values, std::string *data) {
  for (const KVPair &kv : values) {
    uint32_t len = kv.first.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(kv.first.data(), kv.first.size());
    len = kv.second.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(kv.second.data(), kv.second.size());
  }
}

void LeveldbDB::DeserializeRowFilter(std::vector<KVPair> *values, const std::string &data,
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

void LeveldbDB::DeserializeRow(std::vector<KVPair> *values, const std::string &data) {
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

std::string LeveldbDB::BuildCompKey(const std::string &key, const std::string &field_name) {
  switch (format_) {
    case kRowMajor:
      return key + ":" + field_name;
      break;
    case kColumnMajor:
      return field_name + ":" + key;
      break;
    default:
      throw utils::Exception("wrong format");
  }
}

std::string LeveldbDB::KeyFromCompKey(const std::string &comp_key) {
  size_t idx = comp_key.find(":");
  assert(idx != std::string::npos);
  return comp_key.substr(0, idx);
}

std::string LeveldbDB::FieldFromCompKey(const std::string &comp_key) {
  size_t idx = comp_key.find(":");
  assert(idx != std::string::npos);
  return comp_key.substr(idx + 1);
}

int LeveldbDB::ReadSingleEntry(const std::string &table, const std::string &key,
                               const std::vector<std::string> *fields,
                               std::vector<KVPair> &result) {
  std::string data;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(&result, data, *fields);
  } else {
    DeserializeRow(&result, data);
  }
  return kOK;
}

int LeveldbDB::ScanSingleEntry(const std::string &table, const std::string &key,
                               int len, const std::vector<std::string> *fields,
                               std::vector<std::vector<KVPair>> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.push_back(std::vector<KVPair>());
    std::vector<KVPair> &values = result.back();
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

int LeveldbDB::UpdateSingleEntry(const std::string &table, const std::string &key,
                                 std::vector<KVPair> &values) {
  std::string data;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Get: ") + s.ToString());
  }
  std::vector<KVPair> current_values;
  DeserializeRow(&current_values, data);
  for (KVPair &new_kv : values) {
    bool found = false;
    for (KVPair &cur_kv : current_values) {
      if (cur_kv.first == new_kv.first) {
        found = true;
        cur_kv.second = new_kv.second;
        break;
      }
    }
    assert(found);
  }
  leveldb::WriteOptions wopt;

  data.clear();
  SerializeRow(current_values, &data);
  s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Put: ") + s.ToString());
  }
  return kOK;
}

int LeveldbDB::InsertSingleEntry(const std::string &table, const std::string &key,
                                 std::vector<KVPair> &values) {
  std::string data;
  SerializeRow(values, &data);
  leveldb::WriteOptions wopt;
  leveldb::Status s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Put: ") + s.ToString());
  }
  return kOK;
}

int LeveldbDB::DeleteSingleEntry(const std::string &table, const std::string &key) {
  leveldb::WriteOptions wopt;
  leveldb::Status s = db_->Delete(wopt, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Delete: ") + s.ToString());
  }
  return kOK;
}

int LeveldbDB::ReadCompKeyRM(const std::string &table, const std::string &key,
                             const std::vector<std::string> *fields,
                             std::vector<KVPair> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(key);
  if (fields != nullptr) {
    std::vector<std::string>::const_iterator filter_iter = fields->begin();
    for (int i = 0; i < fieldcount_ && filter_iter != fields->end() && db_iter->Valid(); i++) {
      std::string comp_key = db_iter->key().ToString();
      std::string cur_val = db_iter->value().ToString();
      std::string cur_key = KeyFromCompKey(comp_key);
      std::string cur_field = FieldFromCompKey(comp_key);
      assert(cur_key == key);
      assert(cur_field == std::string("field") + std::to_string(i));

      if (cur_field == *filter_iter) {
        result.push_back({cur_field, cur_val});
        filter_iter++;
      }
      db_iter->Next();
    }
    assert(result.size() == fields->size());
  } else {
    for (int i = 0; i < fieldcount_ && db_iter->Valid(); i++) {
      std::string comp_key = db_iter->key().ToString();
      std::string cur_val = db_iter->value().ToString();
      std::string cur_key = KeyFromCompKey(comp_key);
      std::string cur_field = FieldFromCompKey(comp_key);
      assert(cur_key == key);
      assert(cur_field == std::string("field") + std::to_string(i));

      result.push_back({cur_field, cur_val});
      db_iter->Next();
    }
    assert(result.size() == fieldcount_);
  }
  delete db_iter;
  return kOK;
}

int LeveldbDB::ScanCompKeyRM(const std::string &table, const std::string &key,
                             int len, const std::vector<std::string> *fields,
                             std::vector<std::vector<KVPair>> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
  db_iter->Seek(key);
  assert(db_iter->Valid() && KeyFromCompKey(db_iter->key().ToString()) == key);
  for (int i = 0; i < len && db_iter->Valid(); i++) {
    result.push_back(std::vector<KVPair>());
    std::vector<KVPair> &values = result.back();
    if (fields != nullptr) {
      std::vector<std::string>::const_iterator filter_iter = fields->begin();
      for (int j = 0; j < fieldcount_ && filter_iter != fields->end() && db_iter->Valid(); j++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_field == std::string("field") + std::to_string(j));

        if (cur_field == *filter_iter) {
          values.push_back({cur_field, cur_val});
          filter_iter++;
        }
        db_iter->Next();
      }
      assert(values.size() == fields->size());
    } else {
      for (int j = 0; j < fieldcount_ && db_iter->Valid(); j++) {
        std::string comp_key = db_iter->key().ToString();
        std::string cur_val = db_iter->value().ToString();
        std::string cur_key = KeyFromCompKey(comp_key);
        std::string cur_field = FieldFromCompKey(comp_key);
        assert(cur_field == std::string("field") + std::to_string(j));

        values.push_back({cur_field, cur_val});
        db_iter->Next();
      }
      assert(values.size() == fieldcount_);
    }
  }
  delete db_iter;
  return kOK;
}

int LeveldbDB::ReadCompKeyCM(const std::string &table, const std::string &key,
                             const std::vector<std::string> *fields,
                             std::vector<KVPair> &result) {
  throw utils::Exception("not implemented");
  return kOK;
}

int LeveldbDB::ScanCompKeyCM(const std::string &table, const std::string &key,
                             int len, const std::vector<std::string> *fields,
                             std::vector<std::vector<KVPair>> &result) {
  throw utils::Exception("not implemented");
  return kOK;
}

int LeveldbDB::InsertCompKey(const std::string &table, const std::string &key,
                             std::vector<KVPair> &values) {
  leveldb::WriteOptions wopt;
  leveldb::WriteBatch batch;

  std::string comp_key;
  for (KVPair &kv : values) {
    comp_key = BuildCompKey(key, kv.first);
    batch.Put(comp_key, kv.second);
  }

  leveldb::Status s = db_->Write(wopt, &batch);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
  }
  return kOK;
}

int LeveldbDB::DeleteCompKey(const std::string &table, const std::string &key) {
  leveldb::WriteOptions wopt;
  leveldb::WriteBatch batch;

  std::string comp_key;
  for (int i = 0; i < fieldcount_; i++) {
    comp_key = BuildCompKey(key, std::string("field") + std::to_string(i));
    batch.Delete(comp_key);
  }

  leveldb::Status s = db_->Write(wopt, &batch);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
  }
  return kOK;
}

} // ycsbc
