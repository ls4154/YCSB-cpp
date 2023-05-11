#include "checking_workload.h"

#include <string>

#include "workload_factory.h"

namespace {
using namespace std;
const string PROP_CHECKING_COUNT = "checkingcount";
const string PROP_CHECKING_COUNT_DEFAULT = "1000";
};  // namespace

namespace ycsbc {

void CheckingWorkload::Init(const utils::Properties &p) {
    CoreWorkload::Init(p);
  stop_at_ = std::stoi(p.GetProperty(PROP_CHECKING_COUNT, PROP_CHECKING_COUNT_DEFAULT));
}

bool CheckingWorkload::DoInsert(DB &db, ThreadState *_state) {
  bool ret = CoreWorkload::DoInsert(db, _state);
  if (tx_count_.fetch_add(1) == stop_at_) exit(1);
  return ret;
}

bool CheckingWorkload::DoTransaction(DB &db, ThreadState *_state) {
  const std::string key = BuildKeyName(insert_key_sequence_->Next());
  std::vector<DB::Field> result;
  DB::Status status;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    status = db.Read(table_name_, key, &fields, result);
  } else {
    status = db.Read(table_name_, key, NULL, result);
  }
  if (status == DB::kOK) {
    correct_count_.fetch_add(1);
    return true;
  } else {
    return false;
  }
}

const bool registered = WorkloadFactory::RegisterWorkload(
    "com.yahoo.ycsb.workloads.CheckingWorkload", []() { return dynamic_cast<CoreWorkload *>(new CheckingWorkload); });

};  // namespace ycsbc