#include "lazylog_db.h"

#include "core/db_factory.h"
#include "core/utils.h"

namespace ycsbc {

LazylogDB::LazylogDB() { lzlog_ = std::make_shared<lazylog::LazyLogClient>(); }

void LazylogDB::Init() {
    lazylog::Properties props;
    props.FromMap(props_->ToMap());

    lzlog_->Initialize(props);
}

DB::Status LazylogDB::Insert(const std::string& table, const std::string& key, std::vector<Field>& values) {
    std::string data;
    uint32_t key_len = key.size();
    data.append(reinterpret_cast<char *>(&key_len), sizeof(key_len));
    SerializeRow(values, data);

    auto reqid = lzlog_->AppendEntry(data);
    if (reqid.first == 0)
        return Status::kError;
    else
        return Status::kOK;
}

void LazylogDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.first.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.first.data(), field.first.size());
    len = field.second.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.second.data(), field.second.size());
  }
}

const bool registered = DBFactory::RegisterDB("lazylog", []() {
  return static_cast<DB *>(new LazylogDB);
});

}  // namespace ycsbc
