//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "basic_db.h"

using std::cout;
using std::endl;


namespace ycsbc {

void BasicDB::Init() {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "A new thread begins working." << endl;
}

int BasicDB::Read(const std::string &table, const std::string &key,
         const std::vector<std::string> *fields,
         std::vector<Field> &result) {
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
  return 0;
}

int BasicDB::Scan(const std::string &table, const std::string &key,
         int len, const std::vector<std::string> *fields,
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
  return 0;
}

int BasicDB::Update(const std::string &table, const std::string &key,
           std::vector<Field> &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "UPDATE " << table << ' ' << key << " [ ";
  for (auto v : values) {
    cout << v.name << '=' << v.value << ' ';
  }
  cout << ']' << endl;
  return 0;
}

int BasicDB::Insert(const std::string &table, const std::string &key,
           std::vector<Field> &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "INSERT " << table << ' ' << key << " [ ";
  for (auto v : values) {
    cout << v.name << '=' << v.value << ' ';
  }
  cout << ']' << endl;
  return 0;
}

int BasicDB::Delete(const std::string &table, const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  cout << "DELETE " << table << ' ' << key << endl;
  return 0; 
}

DB *NewBasicDB() {
  return new BasicDB;
}

} // ycsbc
