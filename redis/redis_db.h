//
//  redis_db.h
//  YCSB-C
//

#ifndef YCSB_C_REDIS_DB_H_
#define YCSB_C_REDIS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <memory>
#include "core/properties.h"
#include <sw/redis++/redis++.h>

using std::cout;
using std::endl;
using std::shared_ptr;

namespace ycsbc {

using sw::redis::Redis;

class RedisDB : public DB {
 public:
  void Init() override;

  Status Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<Field>> &result) {
    throw "Scan: function not implemented!";
  }

  Status Update(const std::string &table, const std::string &key,
             std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key,
             std::vector<Field> &values) {
    return Update(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    redis_->del(key);
    return DB::kOK;
  }

 private:
  shared_ptr<Redis> redis_;
};

} // ycsbc

#endif // YCSB_C_REDIS_DB_H_
