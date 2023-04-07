#include "rocksdb_svr.h"

#include <signal.h>

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "core/db.h"
#include "core/properties.h"
#include "core/utils.h"
#include "rpc.h"

#define DEBUG 0

using namespace ycsbc;

static bool run = true;

std::mutex mu_;
static std::shared_ptr<rocksdb::Env> env_guard;
static std::shared_ptr<rocksdb::Cache> block_cache;
static std::shared_ptr<rocksdb::Cache> block_cache_compressed;
rocksdb::DB *ServerContext::db_ = nullptr;

void server_func(erpc::Nexus *nexus, int thread_id, const utils::Properties *props);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props);
void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);

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

  nexus.register_req_func(READ_REQ, read_handler);
  nexus.register_req_func(PUT_REQ, put_handler);
  nexus.register_req_func(DELETE_REQ, delete_handler);
  nexus.register_req_func(SCAN_REQ, scan_handler);

  const int n_threads = std::stoi(props.GetProperty("threadcount", "1"));
  std::vector<std::thread> server_threads;

  for (int i = 0; i < n_threads; i++) {
    server_threads.emplace_back(std::move(std::thread(server_func, &nexus, i, &props)));
  }

  for (auto &t : server_threads) {
    t.join();
  }

  delete ServerContext::db_;

  return 0;
}

std::string DeserializeKey(const char *p) {
  uint32_t len = *reinterpret_cast<const uint32_t *>(p);
  std::string key(p + sizeof(uint32_t), len);
  return key;
}

void read_handler(erpc::ReqHandle *req_handle, void *context) {
  auto *rpc = static_cast<ServerContext *>(context)->rpc_;
  auto *db = static_cast<ServerContext *>(context)->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto &resp = static_cast<ServerContext *>(context)->resp_buf_;
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  std::string data;
#if DEBUG
  std::cout << "[READ] key: " << key << std::endl;
#endif

  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &data);

  if (s.IsNotFound()) {
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kNotFound;
  } else if (!s.ok()) {
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
  } else {
    rpc->resize_msg_buffer(&resp, sizeof(DB::Status) + data.size());
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
    memcpy(resp.buf_ + sizeof(DB::Status), data.data(), data.size());
  }
  rpc->enqueue_response(req_handle, &resp);
}

void put_handler(erpc::ReqHandle *req_handle, void *context) {
  auto *rpc = static_cast<ServerContext *>(context)->rpc_;
  auto *db = static_cast<ServerContext *>(context)->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  size_t offset = key.size() + sizeof(uint32_t);
  std::string value(reinterpret_cast<char *>(req->buf_ + offset), req_size - offset);
#if DEBUG
  std::cout << "[INSERT] key: " << key << " value: " << value << std::endl;
#endif

  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db->Put(wopt, key, value);

  auto &resp = req_handle->pre_resp_msgbuf_;
  rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
  if (s.ok()) {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
  } else {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
  }
  rpc->enqueue_response(req_handle, &resp);
}

void delete_handler(erpc::ReqHandle *req_handle, void *context) {
  auto *rpc = static_cast<ServerContext *>(context)->rpc_;
  auto *db = static_cast<ServerContext *>(context)->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));

  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db->Delete(wopt, key);

  auto &resp = req_handle->pre_resp_msgbuf_;
  rpc->resize_msg_buffer(&resp, sizeof(DB::Status));
  if (s.ok()) {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kOK;
  } else {
    *reinterpret_cast<DB::Status *>(resp.buf_) = DB::Status::kError;
  }
  rpc->enqueue_response(req_handle, &resp);
}

void scan_handler(erpc::ReqHandle *req_handle, void *context) {
  auto *rpc = static_cast<ServerContext *>(context)->rpc_;
  auto *db = static_cast<ServerContext *>(context)->db_;
  auto *req = req_handle->get_req_msgbuf();
  auto &resp = static_cast<ServerContext *>(context)->resp_buf_;
  auto req_size = req->get_data_size();
  std::string key = DeserializeKey(reinterpret_cast<const char *>(req->buf_));
  int len = *reinterpret_cast<int *>(req->buf_ + sizeof(uint32_t) + key.size());

  rocksdb::Iterator *db_iter = db->NewIterator(rocksdb::ReadOptions());
  db_iter->Seek(key);
  
  size_t offset = sizeof(DB::Status);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    *reinterpret_cast<uint32_t *>(resp.buf_ + offset) = data.size();
    offset += sizeof(uint32_t);
    memcpy(resp.buf_ + offset, data.data(), data.size());
    offset += data.size();

    db_iter->Next();
  }
  delete db_iter;
  *reinterpret_cast<DB::Status *>(resp.buf_) = DB::kOK;
  rpc->resize_msg_buffer(&resp, offset);
  rpc->enqueue_response(req_handle, &resp);
}

void server_func(erpc::Nexus *nexus, int thread_id, const utils::Properties *props) {
  ServerContext c;
  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c), thread_id, svr_sm_handler, 0x00);
  c.rpc_ = &rpc;
  c.thread_id_ = thread_id;

  {
    std::lock_guard<std::mutex> lock(mu_);

    if (!ServerContext::db_) {  // only create rocksdb once
      const std::string &db_path = props->GetProperty(PROP_NAME, PROP_NAME_DEFAULT);

      rocksdb::Options opt;
      opt.create_if_missing = true;
      std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
      std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
      GetOptions(*props, &opt, &cf_descs);

      rocksdb::Status s;
      if (props->GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
        s = rocksdb::DestroyDB(db_path, opt);
        if (!s.ok()) throw utils::Exception(std::string("RocksDB DestroyDB: ") + s.ToString());
      }
      if (cf_descs.empty()) {
        s = rocksdb::DB::Open(opt, db_path, &ServerContext::db_);
      } else {
        s = rocksdb::DB::Open(opt, db_path, cf_descs, &cf_handles, &ServerContext::db_);
      }
      if (!s.ok()) {
        throw utils::Exception(std::string("RocksDB Open: ") + s.ToString());
      }
      std::cout << "RocksDB opened" << std::endl;
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

void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs) {
  std::string env_uri = props.GetProperty(PROP_ENV_URI, PROP_ENV_URI_DEFAULT);
  std::string fs_uri = props.GetProperty(PROP_FS_URI, PROP_FS_URI_DEFAULT);
  rocksdb::Env *env = rocksdb::Env::Default();
  ;
  if (!env_uri.empty() || !fs_uri.empty()) {
    rocksdb::Status s = rocksdb::Env::CreateFromUri(rocksdb::ConfigOptions(), env_uri, fs_uri, &env, &env_guard);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB CreateFromUri: ") + s.ToString());
    }
    opt->env = env;
  }

  const std::string options_file = props.GetProperty(PROP_OPTIONS_FILE, PROP_OPTIONS_FILE_DEFAULT);
  if (options_file != "") {
    rocksdb::Status s = rocksdb::LoadOptionsFromFile(options_file, env, opt, cf_descs);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB LoadOptionsFromFile: ") + s.ToString());
    }
  } else {
    const std::string compression_type = props.GetProperty(PROP_COMPRESSION, PROP_COMPRESSION_DEFAULT);
    if (compression_type == "no") {
      opt->compression = rocksdb::kNoCompression;
    } else if (compression_type == "snappy") {
      opt->compression = rocksdb::kSnappyCompression;
    } else if (compression_type == "zlib") {
      opt->compression = rocksdb::kZlibCompression;
    } else if (compression_type == "bzip2") {
      opt->compression = rocksdb::kBZip2Compression;
    } else if (compression_type == "lz4") {
      opt->compression = rocksdb::kLZ4Compression;
    } else if (compression_type == "lz4hc") {
      opt->compression = rocksdb::kLZ4HCCompression;
    } else if (compression_type == "xpress") {
      opt->compression = rocksdb::kXpressCompression;
    } else if (compression_type == "zstd") {
      opt->compression = rocksdb::kZSTD;
    } else {
      throw utils::Exception("Unknown compression type");
    }

    int val = std::stoi(props.GetProperty(PROP_MAX_BG_JOBS, PROP_MAX_BG_JOBS_DEFAULT));
    if (val != 0) {
      opt->max_background_jobs = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_BASE, PROP_TARGET_FILE_SIZE_BASE_DEFAULT));
    if (val != 0) {
      opt->target_file_size_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_MULT, PROP_TARGET_FILE_SIZE_MULT_DEFAULT));
    if (val != 0) {
      opt->target_file_size_multiplier = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_BYTES_FOR_LEVEL_BASE, PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT));
    if (val != 0) {
      opt->max_bytes_for_level_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
    if (val != 0) {
      opt->write_buffer_size = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_WRITE_BUFFER, PROP_MAX_WRITE_BUFFER_DEFAULT));
    if (val != 0) {
      opt->max_write_buffer_number = val;
    }
    val = std::stoi(props.GetProperty(PROP_COMPACTION_PRI, PROP_COMPACTION_PRI_DEFAULT));
    if (val != -1) {
      opt->compaction_pri = static_cast<rocksdb::CompactionPri>(val);
    }
    val = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES, PROP_MAX_OPEN_FILES_DEFAULT));
    if (val != 0) {
      opt->max_open_files = val;
    }

    val = std::stoi(props.GetProperty(PROP_L0_COMPACTION_TRIGGER, PROP_L0_COMPACTION_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_file_num_compaction_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_SLOWDOWN_TRIGGER, PROP_L0_SLOWDOWN_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_slowdown_writes_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_STOP_TRIGGER, PROP_L0_STOP_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_stop_writes_trigger = val;
    }

    if (props.GetProperty(PROP_USE_DIRECT_WRITE, PROP_USE_DIRECT_WRITE_DEFAULT) == "true") {
      opt->use_direct_io_for_flush_and_compaction = true;
    }
    if (props.GetProperty(PROP_USE_DIRECT_READ, PROP_USE_DIRECT_READ_DEFAULT) == "true") {
      opt->use_direct_reads = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_WRITE, PROP_USE_MMAP_WRITE_DEFAULT) == "true") {
      opt->allow_mmap_writes = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_READ, PROP_USE_MMAP_READ_DEFAULT) == "true") {
      opt->allow_mmap_reads = true;
    }

    rocksdb::BlockBasedTableOptions table_options;
    size_t cache_size = std::stoul(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
    if (cache_size > 0) {
      block_cache = rocksdb::NewLRUCache(cache_size);
      table_options.block_cache = block_cache;
    }
    size_t compressed_cache_size =
        std::stoul(props.GetProperty(PROP_COMPRESSED_CACHE_SIZE, PROP_COMPRESSED_CACHE_SIZE_DEFAULT));
    if (compressed_cache_size > 0) {
      block_cache_compressed = rocksdb::NewLRUCache(cache_size);
      table_options.block_cache_compressed = rocksdb::NewLRUCache(compressed_cache_size);
    }
    int bloom_bits = std::stoul(props.GetProperty(PROP_BLOOM_BITS, PROP_BLOOM_BITS_DEFAULT));
    if (bloom_bits > 0) {
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloom_bits));
    }
    opt->table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    if (props.GetProperty(PROP_INCREASE_PARALLELISM, PROP_INCREASE_PARALLELISM_DEFAULT) == "true") {
      opt->IncreaseParallelism();
    }
    if (props.GetProperty(PROP_OPTIMIZE_LEVELCOMP, PROP_OPTIMIZE_LEVELCOMP_DEFAULT) == "true") {
      opt->OptimizeLevelStyleCompaction();
    }
  }
}

void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-load") == 0) {
      props.SetProperty("doload", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0 || strcmp(argv[argindex], "-t") == 0) {
      props.SetProperty("dotransaction", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const std::string &message) {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)"
                  << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)), ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
    } else {
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    exit(0);
  }
}

inline bool StrStartWith(const char *str, const char *pre) { return strncmp(str, pre, strlen(pre)) == 0; }
