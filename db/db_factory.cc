//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <string>

#include "db/db_factory.h"
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/leveldb/leveldb_db.h"

namespace ycsbc {

DB* DBFactory::CreateDB(utils::Properties &props) {
  std::string prop_dbname = props.GetProperty("dbname", "basic");
  if (prop_dbname == "basic") {
    return new BasicDB;
  } else if (prop_dbname == "lock_stl") {
    return new LockStlDB;
  } else if (prop_dbname == "leveldb") {
    return new LeveldbDB(props);
  } else {
    return nullptr;
  }
}

} // ycsbc
