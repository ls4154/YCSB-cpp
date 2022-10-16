#include "pure_insert_workload.h"
#include "random_byte_generator.h"
#include "workload_factory.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>

namespace ycsbc {

using std::filesystem::path;

const size_t PureInsertWorkload::RECORD_LENGTH = 27;  // I user21424693888996579940\n
const size_t PureInsertWorkload::KEY_LENGTH = 24;  // user21424693888996579940
const size_t PureInsertWorkload::KEY_OFFSET = 2;  // I<space>

void PureInsertWorkload::Init(const utils::Properties &p) {
  CoreWorkload::Init(p);
  value_len = std::stoi(p.GetProperty("valuelength", "100"));
}

bool PureInsertWorkload::InitThread(const utils::Properties &p, const int mythreadid, const int threadcount) {
  struct stat status;
  int fd;

  std::cout << "init thread " << mythreadid << ", total " << threadcount << std::endl;
  const path workload_path(p.GetProperty("workloadpath", "."));
  const path workload_name("run.w." + std::to_string(mythreadid + 1));

  if ((fd = ::open((workload_path / workload_name).c_str(), O_RDONLY)) < 0) {
    std::cerr << "unable to read file: " << (workload_path / workload_name).c_str() << std::endl;
    return false;
  }

  if (::fstat(fd, &status) < 0) {
    std::cerr << "unable to get file size" << std::endl;
    ::close(fd);
    return false;
  }

  len = status.st_size;
  if ((workload = (char *)::mmap(0, len, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
    std::cerr << "unable to mmap file" << std::endl;
  }
  ::close(fd);

  return true;
}

bool PureInsertWorkload::DoInsert(DB &db) {
  const std::string key = GetNextKey();
  std::vector<DB::Field> value;
  BuildSingleValueOfLen(value, value_len);
  return db.Insert(table_name_, key, value) == DB::kOK;
}

bool PureInsertWorkload::DoTransaction(DB &db) { return DoInsert(db); }

PureInsertWorkload::~PureInsertWorkload() {
  ::munmap(workload, len);
}

bool PureInsertWorkload::HasNextKey() {
  return offset + RECORD_LENGTH <= len;
}

std::string PureInsertWorkload::GetNextKey() {
  if (!HasNextKey()) {
    offset = 0;  // go back to the beginning
  }
  auto key = std::string(workload + offset + KEY_OFFSET, KEY_LENGTH);
  offset += RECORD_LENGTH;
  return key;
}

void PureInsertWorkload::BuildSingleValueOfLen(std::vector<ycsbc::DB::Field> &values, const int val_len) {
  values.push_back(DB::Field());
  ycsbc::DB::Field &field = values.back();
  // field.name = "";
  field.value.reserve(val_len);
  RandomByteGenerator byte_generator;
  std::generate_n(std::back_inserter(field.value), val_len, [&]() { return byte_generator.Next(); } );
}

CoreWorkload *NewPureInsertWorkload() {
  return new PureInsertWorkload;
}

const bool xxx = WorkloadFactory::RegisterWorkload(
  "com.yahoo.ycsb.workloads.PureInsertWorkload",
  NewPureInsertWorkload
);

}  // namespace ycsbc
