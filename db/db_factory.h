//
//  db_factory.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_FACTORY_H_
#define YCSB_C_DB_FACTORY_H_

#include "core/db.h"
#include "core/measurements.h"
#include "core/properties.h"

namespace ycsbc {

class DBFactory {
 public:
  static DB *CreateDB(utils::Properties *props, Measurements *measurements);
};

} // ycsbc

#endif // YCSB_C_DB_FACTORY_H_

