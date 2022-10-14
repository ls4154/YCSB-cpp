#ifndef YCSB_C_WORKLOAD_FACTORY_H_
#define YCSB_C_WORKLOAD_FACTORY_H_

#include "properties.h"
#include "core_workload.h"

#include <map>
#include <string>

namespace ycsbc
{

class WorkloadFactory {
public:
    using WorkloadCreator = CoreWorkload *(*) ();
    static bool RegisterWorkload(std::string name, WorkloadCreator creator);
    static CoreWorkload *CreateWorkload(const utils::Properties &p);
private:
    static std::map<std::string, WorkloadCreator> &Registry();  // Meyer's singleton
};
    
} // namespace ycsbc

#endif
