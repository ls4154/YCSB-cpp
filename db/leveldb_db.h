//
//  leveldb_db.h
//  YCSB-C
//
//  Created by YJ Lee.
//

#ifndef YCSB_C_LEVELDB_DB_H_
#define YCSB_C_LEVELDB_DB_H_

#include <iostream>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <string>

#include "core/db.h"
#include "core/properties.h"

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

namespace ycsbc {

// TODO table handling (ycsb core use only single table)
class LeveldbDB : public DB {
 public:
  LeveldbDB(const utils::Properties &props);

  ~LeveldbDB();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  int Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:
  enum LdbFormat {
    kSingleEntry,
    kRowMajor,
    kColumnMajor
  };
  LdbFormat format_;
  void GetOptions(const utils::Properties &props, leveldb::Options *opt);
  void SerializeRow(std::vector<KVPair> &values, std::string *data);
  void DeserializeRow(std::vector<KVPair> *values, std::string &data);

  int ReadSingleEntry(const std::string &table, const std::string &key,
               const std::vector<std::string> *fields,
               std::vector<KVPair> &result);
  int ScanSingleEntry(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int UpdateSingleEntry(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int InsertSingleEntry(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int DeleteSingleEntry(const std::string &table, const std::string &key);

  leveldb::DB *db_;
  int (LeveldbDB::*method_read_)(const std::string &, const std:: string &,
                                 const std::vector<std::string> *,
                                 std::vector<KVPair> &);
  int (LeveldbDB::*method_scan_)(const std::string &, const std::string &,
                                 int, const std::vector<std::string> *,
                                 std::vector<std::vector<KVPair>> &);
  int (LeveldbDB::*method_update_)(const std::string &, const std::string &,
                                   std::vector<KVPair> &);
  int (LeveldbDB::*method_insert_)(const std::string &, const std::string &,
                                   std::vector<KVPair> &);
  int (LeveldbDB::*method_delete_)(const std::string &, const std::string &);

};

} // ycsbc

#endif // YCSB_C_LEVELDB_DB_H_

