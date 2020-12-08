//
//  basic_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <string>
#include <map>

#include "db_factory.h"
#include "basic_db.h"
#include "db_wrapper.h"

#ifdef BIND_LEVELDB
#include "leveldb/leveldb_db.h"
#endif

#ifdef BIND_ROCKSDB
#include "rocksdb/rocksdb_db.h"
#endif

namespace ycsbc {

static std::map<std::string, DB *(*)()> db_list = {
#ifdef BIND_LEVELDB
  { "leveldb", NewLeveldbDB },
#endif
#ifdef BIND_ROCKSDB
  { "rocksdb", NewRocksdbDB },
#endif
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
