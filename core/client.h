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
                        CountDownLatch *latch, CountDownLatch *init_latch) {
  if (init_db) {
    db->Init();
  }

  ThreadState *thread_state = wl->InitThread(p, thread_id, thread_count);
  init_latch->CountDown();

  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
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
  return oks;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
