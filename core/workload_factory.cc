#include "workload_factory.h"

namespace ycsbc {

const std::string WorkloadFactory::WORKLOAD_NAME_PROPERTY = "workload";
const std::string WorkloadFactory::WORKLOAD_NAME_DEFAULT = "com.yahoo.ycsb.workloads.CoreWorkload";

std::map<std::string, WorkloadFactory::WorkloadCreator> &WorkloadFactory::Registry() {
    static std::map<std::string, WorkloadCreator> registry;
    return registry;
}

bool WorkloadFactory::RegisterWorkload(std::string name, WorkloadCreator creator) {
    Registry()[name] = creator;
    return true;
}

CoreWorkload *WorkloadFactory::CreateWorkload(const utils::Properties &prop) {
  std::string workload_name = prop.GetProperty(WORKLOAD_NAME_PROPERTY, WORKLOAD_NAME_DEFAULT);
  auto &registry = Registry();
  if (registry.find(workload_name) == registry.end()) {
    return nullptr;
  }

  return (*registry[workload_name])();
}

}  // namespace ycsbc
