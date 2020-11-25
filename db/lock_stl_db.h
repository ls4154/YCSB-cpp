//
//  lock_stl_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_LOCK_STL_DB_H_
#define YCSB_C_LOCK_STL_DB_H_

#include "db/hashtable_db.h"

#include <string>
#include <vector>
#include "lib/lock_stl_hashtable.h"

namespace ycsbc {

class LockStlDB : public HashtableDB {
 public:
  LockStlDB() : HashtableDB(new vmp::LockStlHashtable<HashtableDB::FieldHashtable *>) {}

  ~LockStlDB();

 protected:
  HashtableDB::FieldHashtable *NewFieldHashtable();

  void DeleteFieldHashtable(HashtableDB::FieldHashtable *table);

  const char *CopyString(const std::string &str);

  void DeleteString(const char *str);
};

DB *NewLockStlDB();

} // ycsbc

#endif // YCSB_C_LOCK_STL_DB_H_
