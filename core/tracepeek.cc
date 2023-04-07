#include "tracepeek.h"

#include <string>

#include "workload_factory.h"

namespace ycsbc {

void TracePeek::Init(const utils::Properties &p) { CoreWorkload::Init(p); }

bool TracePeek::DoTransaction(DB &_db, ThreadState *state) {
  uint64_t key_num = NextTransactionKeyNum();
  PeekThreadState *peek_state = dynamic_cast<PeekThreadState *>(state);
  peek_state->trace_fs << kOperationString[op_chooser_.Next()] << " " << key_num << std::endl;
  return true;
}

ThreadState *TracePeek::InitThread(const utils::Properties &p, const int mythreadid, const int threadcount,
                                   const int num_ops) {
  return new PeekThreadState(mythreadid);
}

const bool registered = ycsbc::WorkloadFactory::RegisterWorkload("TracePeek", []() {
  return dynamic_cast<CoreWorkload *>(new TracePeek);
});

};  // namespace ycsbc
