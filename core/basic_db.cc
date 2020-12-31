//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "basic_db.h"
#include "core/db_factory.h"

using std::cout;
using std::endl;

namespace ycsbc {

std::mutex BasicDB:: mutex_;

void BasicDB::Init() {
  std::lock_guard<std::mutex> lock(mutex_);
}

DB::Status BasicDB::Read(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields, std::vector<Field> &result) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "READ " << table << ' ' << key;
  if (fields) {
    cout << " [ ";
    for (auto f : *fields) {
      cout << f << ' ';
    }
    cout << ']' << endl;
  } else {
    cout  << " < all fields >" << endl;
  }
  return kOK;
}

DB::Status BasicDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::vector<std::string> *fields,
                         std::vector<std::vector<Field>> &result) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "SCAN " << table << ' ' << key << " " << len;
  if (fields) {
    cout << " [ ";
    for (auto f : *fields) {
      cout << f << ' ';
    }
    cout << ']' << endl;
  } else {
    cout  << " < all fields >" << endl;
  }
  return kOK;
}

DB::Status BasicDB::Update(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "UPDATE " << table << ' ' << key << " [ ";
  for (auto v : values) {
    cout << v.name << '=' << v.value << ' ';
  }
  cout << ']' << endl;
  return kOK;
}

DB::Status BasicDB::Insert(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "INSERT " << table << ' ' << key << " [ ";
  for (auto v : values) {
    cout << v.name << '=' << v.value << ' ';
  }
  cout << ']' << endl;
  return kOK;
}

DB::Status BasicDB::Delete(const std::string &table, const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "DELETE " << table << ' ' << key << endl;
  return kOK;
}

DB *NewBasicDB() {
  return new BasicDB;
}

const bool registered = DBFactory::RegisterDB("basic", NewBasicDB);

} // ycsbc
