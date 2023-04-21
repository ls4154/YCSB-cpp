#include "sqlite_svr.h"

#include <signal.h>

#include <mutex>
#include <string>

#include "common.h"
#include "core/command_line.h"
#include "core/db.h"
#include "core/properties.h"

#define DEBUG 0

namespace {
const std::string TABLE_NAME = "usertable";
const std::string PRIMARY_KEY = "YCSB_KEY";
const std::string COLUMN_NAME = "YCSB_VALUE";
};  // namespace

static bool run = true;
std::mutex mu_;
sqlite3 *ServerContext::db_ = nullptr;

using namespace ycsbc;

void server_func(erpc::Nexus *nexus, int thread_id, const utils::Properties *props);

void sigint_handler(int _signum) { run = false; }
void svr_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

int main(const int argc, const char *argv[]) {
  signal(SIGINT, sigint_handler);

  utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const std::string &server_hostname = props.GetProperty(PROP_SVR_HOSTNAME, PROP_SVR_HOSTNAME_DEFAULT);
  const std::string &server_udpport = props.GetProperty(PROP_UDP_PORT_SVR, PROP_UDP_PORT_SVR_DEFAULT);
  const std::string server_uri = server_hostname + ":" + server_udpport;

  erpc::Nexus nexus(server_uri, 0, 0);

  std::cout << "start eRPC server on " << server_uri << std::endl;

  nexus.register_req_func(SQL_READ_REQ, read_handler);
  nexus.register_req_func(SQL_UPDATE_REQ, update_handler);
  nexus.register_req_func(SQL_INSERT_REQ, insert_handler);
  nexus.register_req_func(SQL_DELETE_REQ, delete_handler);

  const int n_threads = std::stoi(props.GetProperty("threadcount", "1"));
  std::vector<std::thread> server_threads;

  for (int i = 0; i < n_threads; i++) {
    server_threads.emplace_back(std::move(std::thread(server_func, &nexus, i, &props)));
  }

  for (auto &t : server_threads) {
    t.join();
  }

  int ret = sqlite3_close(ServerContext::db_);
  if (ret != SQLITE_OK) {
    throw utils::Exception(std::string("Failed to close SQLite: ") + sqlite3_errmsg(ServerContext::db_));
  }

  return 0;
}

std::string DeserializeKey(const char *p) {
  uint32_t len = *reinterpret_cast<const uint32_t *>(p);
  std::string key(p + sizeof(uint32_t), len);
  return key;
}

bool prepareReadQuery(ServerContext *ctx) {
  sqlite3_stmt *stmt;
  const std::string query("SELECT " + COLUMN_NAME + " FROM " + TABLE_NAME + " WHERE " + PRIMARY_KEY + " = ?;");
  int ret = sqlite3_prepare_v2(ServerContext::db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare read query: " << query << ". error: " << sqlite3_errmsg(ServerContext::db_)
              << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  ctx->prepared_stmts_[SQL_READ_REQ] = stmt;
  return true;
}

bool prepareInsertQuery(ServerContext *ctx) {
  sqlite3_stmt *stmt;
  const std::string query("INSERT INTO " + TABLE_NAME + " (" + PRIMARY_KEY + "," + COLUMN_NAME + ") VALUES (?, ?);");
  int ret = sqlite3_prepare_v2(ServerContext::db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare insert query: " << query << ". error: " << sqlite3_errmsg(ServerContext::db_)
              << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  ctx->prepared_stmts_[SQL_INSERT_REQ] = stmt;
  return true;
}

bool prepareUpdateQuery(ServerContext *ctx) {
  sqlite3_stmt *stmt;
  const std::string query("UPDATE " + TABLE_NAME + " SET " + COLUMN_NAME + " = ? WHERE " + PRIMARY_KEY + " = ?;");
  int ret = sqlite3_prepare_v2(ServerContext::db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare update query: " << query << ". error: " << sqlite3_errmsg(ServerContext::db_)
              << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  ctx->prepared_stmts_[SQL_UPDATE_REQ] = stmt;
  return true;
}

bool prepareDeleteQuery(ServerContext *ctx) {
  sqlite3_stmt *stmt;
  const std::string query("DELETE FROM " + TABLE_NAME + " WHERE " + PRIMARY_KEY + " = ?;");
  int ret = sqlite3_prepare_v2(ServerContext::db_, query.c_str(), query.size() + 1, &stmt, NULL);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to prepare delete query: " << query << ". error: " << sqlite3_errmsg(ServerContext::db_)
              << std::endl;
    sqlite3_finalize(stmt);
    return false;
  }
  ctx->prepared_stmts_[SQL_DELETE_REQ] = stmt;
  return true;
}

void read_handler(erpc::ReqHandle *req_handle, void *context) {
  ServerContext *ctx = static_cast<ServerContext *>(context);
  auto *rpc = ctx->rpc_;
  auto *db = ctx->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto &resp = ctx->resp_buf_;
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  sqlite3_stmt *stmt;
  int ret;
#if DEBUG
  std::cout << "[READ] key: " << key << std::endl;
#endif

  if (ctx->prepared_stmts_.find(SQL_READ_REQ) == ctx->prepared_stmts_.end()) {
    if (!prepareReadQuery(ctx)) {
      rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
      *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
      rpc->enqueue_response(req_handle, &resp);
      return;
    }
  }
  stmt = ctx->prepared_stmts_[SQL_READ_REQ];
  ret = sqlite3_bind_text(stmt, 1, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to read query, error: " << sqlite3_errstr(ret) << std::endl;
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
    goto read_resp;
  }

  ret = sqlite3_step(stmt);
  if (ret == SQLITE_ROW) {
    const uint8_t *value = sqlite3_column_text(stmt, 0);
    int value_size = sqlite3_column_bytes(stmt, 0);
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status) + value_size);
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
    memcpy(resp.buf_ + sizeof(DB::Status), value, value_size);
  } else if (ret == SQLITE_DONE) {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kNotFound;
  } else {
#if DEBUG
    std::cerr << "Failed to get column text, error: " << sqlite3_errstr(ret) << std::endl;
#endif
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
  }

read_resp:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset read query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  rpc->enqueue_response(req_handle, &resp);
}

void insert_handler(erpc::ReqHandle *req_handle, void *context) {
  ServerContext *ctx = static_cast<ServerContext *>(context);
  auto *rpc = ctx->rpc_;
  auto *db = ctx->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto &resp = req_handle->pre_resp_msgbuf_;
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  size_t offset = key.size() + sizeof(uint32_t);
  std::string value(reinterpret_cast<char *>(req->buf_ + offset), req_size - offset);
  sqlite3_stmt *stmt;
  int ret;
#if DEBUG
  std::cout << "[INSERT] key: " << key << " value: " << value << std::endl;
#endif

  if (ctx->prepared_stmts_.find(SQL_INSERT_REQ) == ctx->prepared_stmts_.end()) {
    if (!prepareInsertQuery(ctx)) {
      rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
      *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
      rpc->enqueue_response(req_handle, &resp);
      return;
    }
  }
  stmt = ctx->prepared_stmts_[SQL_INSERT_REQ];
  ret = sqlite3_bind_text(stmt, 1, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to insert query, error: " << sqlite3_errstr(ret) << std::endl;
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
    goto insert_resp;
  }
  ret = sqlite3_bind_text(stmt, 2, value.data(), value.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind value to insert query, error: " << sqlite3_errstr(ret) << std::endl;
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
    goto insert_resp;
  }

  ret = sqlite3_step(stmt);
  rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
  if (ret == SQLITE_DONE) {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
  } else {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
#if DEBUG
    std::cerr << "Failed to insert, error: " << sqlite3_errstr(ret) << std::endl;
#endif
  }

insert_resp:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset insert query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  rpc->enqueue_response(req_handle, &resp);
}

void update_handler(erpc::ReqHandle *req_handle, void *context) {
  ServerContext *ctx = static_cast<ServerContext *>(context);
  auto *rpc = ctx->rpc_;
  auto *db = ctx->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto &resp = req_handle->pre_resp_msgbuf_;
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  size_t offset = key.size() + sizeof(uint32_t);
  std::string value(reinterpret_cast<char *>(req->buf_ + offset), req_size - offset);
  sqlite3_stmt *stmt;
  int ret;
#if DEBUG
  std::cout << "[UPDATE] key: " << key << " value: " << value << std::endl;
#endif

  if (ctx->prepared_stmts_.find(SQL_UPDATE_REQ) == ctx->prepared_stmts_.end()) {
    if (!prepareUpdateQuery(ctx)) {
      rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
      *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
      rpc->enqueue_response(req_handle, &resp);
      return;
    }
  }
  stmt = ctx->prepared_stmts_[SQL_UPDATE_REQ];
  ret = sqlite3_bind_text(stmt, 1, value.data(), value.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind value to update query, error: " << sqlite3_errstr(ret) << std::endl;
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
    goto update_resp;
  }
  ret = sqlite3_bind_text(stmt, 2, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to update query, error: " << sqlite3_errstr(ret) << std::endl;
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
    goto update_resp;
  }

  ret = sqlite3_step(stmt);
  rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
  if (ret == SQLITE_DONE) {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
  } else {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
#if DEBUG
    std::cerr << "Failed to update, error: " << sqlite3_errstr(ret) << std::endl;
#endif
  }

update_resp:
  ret = sqlite3_reset(stmt);
  if (ret != SQLITE_OK) {
    std::cerr << "Can't reset update query, error: " << sqlite3_errstr(ret) << std::endl;
  }
  rpc->enqueue_response(req_handle, &resp);
}

void delete_handler(erpc::ReqHandle *req_handle, void *context) {
  ServerContext *ctx = static_cast<ServerContext *>(context);
  auto *rpc = ctx->rpc_;
  auto *db = ctx->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto &resp = req_handle->pre_resp_msgbuf_;
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  sqlite3_stmt *stmt;
  int ret;

  if (ctx->prepared_stmts_.find(SQL_DELETE_REQ) == ctx->prepared_stmts_.end()) {
    if (!prepareDeleteQuery(ctx)) {
      rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
      *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
      rpc->enqueue_response(req_handle, &resp);
      return;
    }
  }
  stmt = ctx->prepared_stmts_[SQL_DELETE_REQ];
  ret = sqlite3_bind_text(stmt, 1, key.data(), key.size(), SQLITE_STATIC);
  if (ret != SQLITE_OK) {
    std::cerr << "Failed to bind key to delete query, error: " << sqlite3_errstr(ret) << std::endl;
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
    goto delete_resp;
  }

  ret = sqlite3_step(stmt);
  rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
  if (ret == SQLITE_DONE) {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
  } else {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
#if DEBUG
    std::cerr << "Failed to delete, error: " << sqlite3_errstr(ret) << std::endl;
#endif
  }

delete_resp:
  rpc->enqueue_response(req_handle, &resp);
}

void server_func(erpc::Nexus *nexus, int thread_id, const utils::Properties *props) {
  ServerContext c;
  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c), thread_id, svr_sm_handler, 0x00);
  c.rpc_ = &rpc;
  c.thread_id_ = thread_id;
  char *err_msg;

  {
    std::lock_guard<std::mutex> lock(mu_);

    if (!ServerContext::db_) {
      int ret;

      ret = sqlite3_open(props->GetProperty(PROP_NAME, PROP_NAME_DEFAULT).c_str(), &c.db_);
      if (ret != SQLITE_OK) {
        sqlite3_close(c.db_);
        throw utils::Exception(std::string("SQLite Open: ") + sqlite3_errmsg(c.db_));
      }

      ret = sqlite3_exec(c.db_, "PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
                         nullptr, nullptr, &err_msg);
      if (ret != SQLITE_OK) {
        sqlite3_close(c.db_);
        throw utils::Exception(std::string("Can't set journal mode to WAL, error: ") + err_msg);
      }

      // create table
      const std::string query("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + PRIMARY_KEY +
                              " TEXT NOT NULL PRIMARY KEY, " + COLUMN_NAME + " TEXT NOT NULL);");
      ret = sqlite3_exec(c.db_, query.c_str(), nullptr, nullptr, &err_msg);
      if (ret != SQLITE_OK) {
        sqlite3_close(c.db_);
        throw utils::Exception(std::string("Can't create table, error: ") + err_msg);
      }
    }
  }

  std::cout << "thread " << thread_id << " start running" << std::endl;

  const int msg_size = std::stoull(props->GetProperty(PROP_MSG_SIZE, PROP_MSG_SIZE_DEFAULT));
  c.resp_buf_ = rpc.alloc_msg_buffer_or_die(msg_size);
  while (run) {
    rpc.run_event_loop(1000);
  }
  rpc.free_msg_buffer(c.resp_buf_);

  std::cout << "thread " << thread_id << " stop running" << std::endl;
}
