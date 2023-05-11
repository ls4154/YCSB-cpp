#ifndef YCSB_C_CHECKING_WORKLOAD_H_
#define YCSB_C_CHECKING_WORKLOAD_H_

#include "core_workload.h"

#include <atomic>

namespace ycsbc {

class CheckingWorkload : public CoreWorkload {
public:
    CheckingWorkload() : CoreWorkload(), stop_at_(0), tx_count_(0), correct_count_(0) {}
    ~CheckingWorkload() override {}

    void Init(const utils::Properties &p) override;
    bool DoInsert(DB &db, ThreadState *state) override;
    bool DoTransaction(DB &db, ThreadState *state) override;

protected:
    int stop_at_;
    std::atomic<int> tx_count_;
    std::atomic<int> correct_count_;
};

};

#endif
