#include "wiredtiger_db.h"

#include "core/properties.h"
#include "core/utils.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>

#define WT_PREFIX "wiredtiger"
#define STR(x) #x

#define error_check(x) { \
  if((x) != 0){ \
    throw utils::Exception(std::string("[" WT_PREFIX "]" __FILE__ ":" STR(__LINE__)));  \
  } \
}

namespace {
  const std::string PROP_HOME = WT_PREFIX ".home";
  const std::string PROP_HOME_DEFAULT = "";
  
  const std::string PROP_COMPRESSION = WT_PREFIX ".compression";
  const std::string PROP_COMPRESSION_DEFAULT = "";

  const std::string PROP_FORMAT = WT_PREFIX ".format";
  const std::string PROP_FORMAT_DEFAULT = "single";
}

namespace ycsbc {

WT_CONNECTION* WTDB::conn_ = nullptr;
int WTDB::ref_cnt_ = 0;
std::mutex WTDB::mu_;

void WTDB::Init(){
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string &format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  if(format=="single"){
    method_read_ = &WTDB::ReadSingleEntry;
    method_scan_ = &WTDB::ScanSingleEntry;
    method_update_ = &WTDB::UpdateSingleEntry;
    method_insert_ = &WTDB::InsertSingleEntry;
    method_delete_ = &WTDB::DeleteSingleEntry;
  } else {
    throw utils::Exception("single ONLY");
  }

  ref_cnt_++;
  if(conn_){
    error_check(conn_->open_session(conn_, NULL, NULL, &session_));
    error_check(session_->open_cursor(session_, "table:ycsbc", NULL, "overwrite=true", &cursor_));
    return;
  }

  const std::string &home = props.GetProperty(PROP_HOME, PROP_HOME_DEFAULT);
  if(home.empty()){
    throw utils::Exception(WT_PREFIX " home is missing");
  }
  char* create_home_dir = (char*)malloc(20+2*home.size());
  std::sprintf(create_home_dir, "rm -rf %s && mkdir %s", home.c_str(), home.c_str());
  error_check(system(create_home_dir));
  free(create_home_dir);
  error_check(wiredtiger_open(home.c_str(), NULL, "create", &conn_));

  error_check(conn_->open_session(conn_, NULL, NULL, &session_));
  error_check(session_->create(session_, "table:ycsbc", "key_format=u,value_format=u"));
  error_check(session_->open_cursor(session_, "table:ycsbc", NULL, "overwrite=true", &cursor_));
}

void WTDB::Cleanup(){
  const std::lock_guard<std::mutex> lock(mu_);
  cursor_->close(cursor_);
  error_check(session_->close(session_, NULL));
  if (--ref_cnt_) {
    return;
  }
  error_check(conn_->close(conn_, NULL));
}

DB::Status WTDB::ReadSingleEntry(const std::string &table, const std::string &key,
                                      const std::vector<std::string> *fields,
                                      std::vector<Field> &result) {
  WT_ITEM k = {.data = key.data(), .size = key.size()};
  WT_ITEM v;
  int ret;
  cursor_->set_key(cursor_, &k);
  ret = cursor_->search(cursor_);
  if(ret==WT_NOTFOUND){
    return kNotFound;
  } else if(ret != 0) {
    throw utils::Exception(WT_PREFIX " search error");
  }
  error_check(cursor_->get_value(cursor_, &v));
  if (fields != nullptr) {
    DeserializeRowFilter(&result, (const char*)v.data, v.size, *fields);
  } else {
    DeserializeRow(&result, (const char*)v.data, v.size);
  }
  return kOK;
}

DB::Status WTDB::ScanSingleEntry(const std::string &table, const std::string &key, int len,
                                      const std::vector<std::string> *fields,
                                      std::vector<std::vector<Field>> &result) {
  WT_ITEM k = {.data = key.data(), .size = key.size()};
  WT_ITEM v;
  int ret = 0, exact;

  cursor_->set_key(cursor_, &k);
  error_check(cursor_->search_near(cursor_, &exact));
  if (exact < 0) {
    ret = cursor_->next(cursor_);
  }
  for(int i=0; !ret && i<len; ++i){
    error_check(cursor_->get_value(cursor_, &v));
    result.emplace_back(std::vector<Field>());
    if (fields != nullptr) {
      DeserializeRowFilter(&result.back(), (const char*)v.data, v.size, *fields);
    } else {
      DeserializeRow(&result.back(), (const char*)v.data, v.size);
    }
  }
  return kOK;
}

DB::Status WTDB::UpdateSingleEntry(const std::string &table, const std::string &key,
                           std::vector<Field> &values){
  std::vector<Field> current_values;
  WT_ITEM k = {.data = key.data(), .size = key.size()};
  WT_ITEM v;
  int ret;

  cursor_->set_key(cursor_, &k);
  ret = cursor_->search(cursor_);
  if(ret==WT_NOTFOUND){
    return kNotFound;
  } else if(ret != 0) {
    throw utils::Exception(WT_PREFIX " search error");
  }
  error_check(cursor_->get_value(cursor_, &v));
  DeserializeRow(&current_values, (const char*)v.data, v.size);
  for (Field &new_field : values) {
    bool found __attribute__((unused)) = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }

  std::string data;
  SerializeRow(current_values, &data);
  v.data = data.data();
  v.size = data.size();
  cursor_->set_value(cursor_, &v);
  ret = cursor_->update(cursor_);
  if(ret==WT_NOTFOUND){
    return kNotFound;
  } else if(ret != 0) {
    throw utils::Exception(WT_PREFIX " update error");
  }
  return kOK;
}

DB::Status WTDB::InsertSingleEntry(const std::string &table, const std::string &key,
                           std::vector<Field> &values){
  std::string data;
  WT_ITEM k = {.data = key.data(), .size = key.size()}, v;
  
  cursor_->set_key(cursor_, &k);
  SerializeRow(values, &data);
  v.data = data.data();
  v.size = data.size();
  cursor_->set_value(cursor_, &v);
  error_check(cursor_->insert(cursor_));
  // TODO: cursor reset?
  return kOK;
}
DB::Status WTDB::DeleteSingleEntry(const std::string &table, const std::string &key){
  WT_ITEM k = {.data = key.data(), .size = key.size()};
  cursor_->set_key(cursor_, &k);
  error_check(cursor_->remove(cursor_));
  return kOK;
}

void WTDB::SerializeRow(const std::vector<Field> &values, std::string *data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.name.data(), field.name.size());
    len = field.value.size();
    data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data->append(field.value.data(), field.value.size());
  }
}

void WTDB::DeserializeRow(std::vector<Field> *values, const char *data_ptr, size_t data_len) {
  const char *p = data_ptr;
  const char *lim = p + data_len;
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values->push_back({field, value});
  }
  assert(values->size() == fieldcount_);
}

void WTDB::DeserializeRowFilter(std::vector<Field> *values, const char *data_ptr, size_t data_len,
                                  const std::vector<std::string> &fields) {
  const char *p = data_ptr;
  const char *lim = p + data_len;
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values->push_back({field, value});
      filter_iter++;
    }
  }
  assert(values->size() == fields.size());
}

DB *NewWTDB() {
  return new WTDB;
}

const bool registered = DBFactory::RegisterDB("wiredtiger", NewWTDB);

}