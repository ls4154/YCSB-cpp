#include "pure_insert_workload.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include "random_byte_generator.h"
#include "workload_factory.h"

namespace ycsbc {

using std::string;

const size_t InsertThreadState::RECORD_LENGTH = 27;  // I user21424693888996579940\n
const size_t InsertThreadState::KEY_LENGTH = 24;     // user21424693888996579940
const size_t InsertThreadState::KEY_OFFSET = 2;      // I<space>

void PureInsertWorkload::Init(const utils::Properties &p) {
  CoreWorkload::Init(p);
  value_len = std::stoi(p.GetProperty("valuelength", "100"));
}

ThreadState *PureInsertWorkload::InitThread(const utils::Properties &p, const int mythreadid, const int threadcount,
                                            const int num_ops) {
  return new InsertThreadState(p, mythreadid, threadcount, num_ops);
}

bool PureInsertWorkload::DoInsert(DB &db, ThreadState *state) {
  InsertThreadState *insert_state = dynamic_cast<InsertThreadState *>(state);
  const std::string key = insert_state->GetNextKey();
  std::vector<DB::Field> value;
  BuildSingleValueOfLen(value, value_len);
  return db.Insert(table_name_, key, value) == DB::kOK;
}

bool PureInsertWorkload::DoTransaction(DB &db, ThreadState *state) { return DoInsert(db, state); }

InsertThreadState::InsertThreadState(const utils::Properties &p, const int mythreadid, const int threadcount,
                                     const int num_ops)
    : cur_workload(nullptr), workload_i(0), offset(0) {
  int loaded_ops = 0;
  char *workload = nullptr;
  std::cout << "init thread " << mythreadid << ", total " << threadcount << std::endl;

  for (int i = 0; loaded_ops < num_ops; i++) {
    struct stat status;
    int fd;

    const string workload_path(p.GetProperty("workloadpath", "."));
    const string workload_name("run.w." + std::to_string(i * threadcount + mythreadid + 1));

    if ((fd = ::open((workload_path + "/" + workload_name).c_str(), O_RDONLY)) < 0) {
      std::cerr << "unable to read file: " << (workload_path + "/" + workload_name).c_str() << std::endl;
    }

    if (::fstat(fd, &status) < 0) {
      std::cerr << "unable to get file size" << std::endl;
      ::close(fd);
    }

    len = status.st_size;
    loaded_ops += len / RECORD_LENGTH;
    if ((workload = (char *)::mmap(0, len, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
      std::cerr << "unable to mmap file" << std::endl;
    }
    workloads.push_back(workload);
    ::close(fd);
  }
  cur_workload = workloads[0];
}

InsertThreadState::~InsertThreadState() {
  for (char *w : workloads) {
    ::munmap(w, len);
  }
}

bool InsertThreadState::HasNextKey() { return offset + RECORD_LENGTH <= len; }

std::string InsertThreadState::GetNextKey() {
  if (!HasNextKey()) {
    workload_i++;  // switch to next workload file
    cur_workload = workloads[workload_i];
    offset = 0;  // go back to the beginning
  }
  auto key = std::string(cur_workload + offset + KEY_OFFSET, KEY_LENGTH);
  offset += RECORD_LENGTH;
  return key;
}

CoreWorkload *NewPureInsertWorkload() { return new PureInsertWorkload; }

const bool xxx =
    WorkloadFactory::RegisterWorkload("com.yahoo.ycsb.workloads.PureInsertWorkload", NewPureInsertWorkload);

}  // namespace ycsbc
