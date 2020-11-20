//
//  leveldb_db.cc
//  YCSB-C
//
//  Created by YJ Lee.
//

#include "leveldb_db.h"
#include "core/utils.h"

#include "leveldb/options.h"

namespace ycsbc {

LeveldbDB::LeveldbDB(const utils::Properties &props) {
  const std::string &db_path = props["ldb_path"];

  leveldb::Options opt;
  opt.create_if_missing = true;
  GetOptions(props, &opt);

  leveldb::Status s;
  s = leveldb::DestroyDB(db_path, opt);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB DestoryDB: ") + s.ToString());
  }
  s = leveldb::DB::Open(opt, db_path, &db_);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Open: ") + s.ToString());
  }

  const std::string &format = props["ldb_format"];
  if (format == "single") {
    format_ = kSingleEntry;
    method_read_ = &LeveldbDB::ReadSingleEntry;
    method_scan_ = &LeveldbDB::ScanSingleEntry;
    method_update_ = &LeveldbDB::UpdateSingleEntry;
    method_insert_ = &LeveldbDB::InsertSingleEntry;
    method_delete_ = &LeveldbDB::DeleteSingleEntry;
  } else if (format == "row") {
    format_ = kRowMajor;
    throw utils::Exception("row major not implemented");
  } else if (format == "column") {
    format_ = kColumnMajor;
    throw utils::Exception("column major not implemented");
  } else {
    throw utils::Exception("unknown format");
  }
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

void LeveldbDB::SerializeRow(std::vector<KVPair> &values, std::string *data) {
  for (KVPair &kv : values) {
    uint32_t len = kv.first.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(kv.first.data(), kv.first.size());
    len = kv.second.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(kv.second.data(), kv.second.size());
  }
}

void LeveldbDB::DeserializeRow(std::vector<KVPair> *values, std::string &data) {
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
    // TODO
    throw utils::Exception(__PRETTY_FUNCTION__);
    for (const std::string &f : *fields) {
      f.data();
    }
  } else {
    DeserializeRow(&result, data);
  }
  return kOK;
}

int LeveldbDB::ScanSingleEntry(const std::string &table, const std::string &key,
                               int len, const std::vector<std::string> *fields,
                               std::vector<std::vector<KVPair>> &result) {
  leveldb::Iterator *it = db_->NewIterator(leveldb::ReadOptions());
  it->Seek(key);
  for (int i = 0; it->Valid() && i < len; i++) {
    if (fields != nullptr) {
      // TODO
      throw utils::Exception(__PRETTY_FUNCTION__);
    } else {
      std::string data = it->value().ToString();
      result.push_back(std::vector<KVPair>());
      std::vector<KVPair> &values = result.back();
      DeserializeRow(&values, data);
      it->Next();
    }
  }
  delete it;
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
    for (KVPair &cur_kv : current_values) {
      if (cur_kv.first == new_kv.first) {
        cur_kv.second = new_kv.second;
        break;
      }
    }
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

} // ycsbc
