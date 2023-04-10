//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include "countdown_latch.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const utils::Properties &p, const int num_ops,
                        const int thread_id, const int thread_count, bool is_loading, bool init_db, bool cleanup_db,
                        CountDownLatch *latch, CountDownLatch *init_latch, int64_t sec_skip) {
  if (init_db) {
    db->Init();
  }

  ThreadState *thread_state = wl->InitThread(p, thread_id, thread_count, num_ops);
  init_latch->CountDown();

  bool count_on = (sec_skip == 0);
  auto start = std::chrono::system_clock::now();
  int skipped_ok = 0;

  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (!count_on) {
      auto elapse = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start).count();
      count_on = (elapse > sec_skip);
      skipped_ok = oks;
    }
    if (is_loading) {
      oks += wl->DoInsert(*db, thread_state);
    } else {
      oks += wl->DoTransaction(*db, thread_state);
    }
  }

  delete thread_state;

  if (cleanup_db) {
    db->Cleanup();
  }

  latch->CountDown();
  return oks - skipped_ok;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
