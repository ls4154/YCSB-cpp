//
//  redis_db.cc
//  YCSB-C
//

#include "redis_db.h"

#include "core/db_factory.h"
#include "core/utils.h"

#include <cstring>
#include <string>

using namespace std;

const string PROP_HOST = "redis.host";
const string PROP_HOST_DEFAULT = "localhost";

const string PROP_PORT = "redis.port";
const string PROP_PORT_DEFAULT = "6379";

namespace ycsbc {

using sw::redis::OptionalString;
using sw::redis::ConnectionOptions;

void RedisDB::Init() {
    const utils::Properties &props = *props_;
    ConnectionOptions conn_opt;

    conn_opt.host = props.GetProperty(PROP_HOST, PROP_HOST_DEFAULT);
    conn_opt.port = stoi(props.GetProperty(PROP_PORT, PROP_PORT_DEFAULT));

    redis_ = make_shared<Redis>(conn_opt);
}

DB::Status RedisDB::Read(const string &table, const string &key,
         const vector<string> *fields,
         vector<Field> &result) {
  if (fields) {
    vector<OptionalString> values;
    redis_->hmget(key, fields->begin(), fields->end(), back_inserter(values));
    assert(fields->size() == values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      const OptionalString &value = values[i];
      result.push_back({fields->at(i), value.has_value() ? value.value() : ""});
    }
  } else {
    vector<string> values;
    redis_->hgetall(key, back_inserter(values));
    assert(values.size() % 2 == 0);
    for (size_t i = 0; i < values.size() / 2; ++i) {
      result.push_back({values[2 * i], values[2 * i + 1]});
    }
  }
  return DB::kOK;
}

DB::Status RedisDB::Update(const string &table, const string &key,
           vector<Field> &values) {
  redis_->hmset(key, values.begin(), values.end());
  return DB::kOK;
}

DB *NewRedisDB() {
    return new RedisDB;
}

const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

} // namespace ycsbc
