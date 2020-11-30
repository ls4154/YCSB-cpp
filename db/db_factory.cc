//
//  basic_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <string>
#include <map>

#include "db/db_factory.h"
#include "db/basic_db.h"
#include "core/db_wrapper.h"
#include "db/lock_stl_db.h"

#ifdef BIND_LEVELDB
#include "db/leveldb/leveldb_db.h"
#endif

#ifdef BIND_ROCKSDB
#include "db/rocksdb/rocksdb_db.h"
#endif

namespace ycsbc {

static std::map<std::string, DB *(*)()> db_list = {
#ifdef BIND_LEVELDB
  { "leveldb", NewLeveldbDB },
#endif
#ifdef BIND_ROCKSDB
  { "rocksdb", NewRocksdbDB },
#endif
  { "lock_stl", NewLockStlDB },
  { "basic", NewBasicDB }
};

DB *DBFactory::CreateDB(utils::Properties *props, Measurements *measurements) {
  std::string prop_dbname = props->GetProperty("dbname", "basic");
  DB *db = nullptr;
  if (db_list.find(prop_dbname) != db_list.end()) {
    DB *new_db = (*db_list[prop_dbname])();
    new_db->SetProps(props);
    db = new DBWrapper(new_db, measurements);
  }
  return db;
}

} // ycsbc
