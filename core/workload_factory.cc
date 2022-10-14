#include "workload_factory.h"

namespace ycsbc {

std::map<std::string, WorkloadFactory::WorkloadCreator> &WorkloadFactory::Registry() {
    static std::map<std::string, WorkloadCreator> registry;
    return registry;
}

bool WorkloadFactory::RegisterWorkload(std::string name, WorkloadCreator creator) {
    Registry()[name] = creator;
    return true;
}

CoreWorkload *WorkloadFactory::CreateWorkload(const utils::Properties &prop) {
  std::string workload_name = prop.GetProperty("workload", "com.yahoo.ycsb.workloads.CoreWorkload");
  auto &registry = Registry();
  if (registry.find(workload_name) == registry.end()) {
    return nullptr;
  }

  return (*registry[workload_name])();
}

}  // namespace ycsbc
