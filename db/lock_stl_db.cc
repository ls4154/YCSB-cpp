//
//  lock_stdl_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "lock_stl_db.h"

namespace ycsbc {
LockStlDB::~LockStlDB() {
  std::vector<KeyHashtable::KVPair> key_pairs = key_table_->Entries();
  for (auto &key_pair : key_pairs) {
    DeleteFieldHashtable(key_pair.second);
  }
  delete key_table_;
}

HashtableDB::FieldHashtable *LockStlDB::NewFieldHashtable() {
  return new vmp::LockStlHashtable<const char *>;
}

void LockStlDB::DeleteFieldHashtable(HashtableDB::FieldHashtable *table) {
  std::vector<FieldHashtable::KVPair> pairs = table->Entries();
  for (auto &pair : pairs) {
    DeleteString(pair.second);
  }
  delete table;
}

const char *LockStlDB::CopyString(const std::string &str) {
  char *value = new char[str.length() + 1];
  strcpy(value, str.c_str());
  return value;
}

void LockStlDB::DeleteString(const char *str) {
  delete[] str;
}

DB *NewLockStlDB() {
  return new LockStlDB;
}

} // ycsbc
