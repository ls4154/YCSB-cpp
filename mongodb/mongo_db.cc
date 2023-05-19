#include "mongo_db.h"
#include "core/db_factory.h"

#include <string>
using namespace std;

namespace {
const string PROP_SERVER_URI = "mongodb.uri";
const string PROP_SERVER_URI_DEFAULT = "mongodb://localhost:27017";
};

namespace ycsbc {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

mongocxx::instance MongoDB::inst_{};

void MongoDB::Init() {
  const utils::Properties &props = *props_;
  
  const auto uri = mongocxx::uri{props.GetProperty(PROP_SERVER_URI, PROP_SERVER_URI_DEFAULT)};
  
  mongocxx::options::client client_options;
  const auto api = mongocxx::options::server_api{mongocxx::options::server_api::version::k_version_1};
  client_options.server_api_opts(api);

  conn_ = new mongocxx::client(uri, client_options);
  db_ = (*conn_)["ycsbdb"];
  collection_ = db_["ycsb"];

  db_.run_command(make_document(kvp("ping", 1)));

  cout << "thread init" << endl;
}

DB::Status MongoDB::Insert(const std::string& table, const std::string& key, std::vector<Field>& values) {
  string data;
  SerializeRow(values, data);

  mongocxx::write_concern wc;
  mongocxx::options::insert ins_opt;
  wc.journal(true);
  ins_opt.write_concern(wc);

  auto res = collection_.insert_one(make_document(kvp("k", key), kvp("v", data)), ins_opt);
  if (res->result().inserted_count() == 1)
    return Status::kOK;
  else
    return Status::kError;
}

void MongoDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.first.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.first.data(), field.first.size());
    len = field.second.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.second.data(), field.second.size());
  }
}

const bool registered = DBFactory::RegisterDB("mongodb", []() {
  return dynamic_cast<DB*>(new MongoDB);
});

};