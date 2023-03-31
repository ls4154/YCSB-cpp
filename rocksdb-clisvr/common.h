#pragma once

#include <stdint.h>

#include <string>

const std::string PROP_UDP_PORT_CLI = "rocksdb-clisvr.udp_port_cli";
const std::string PROP_UDP_PORT_CLI_DEFAULT = "31850";

const std::string PROP_UDP_PORT_SVR = "rocksdb-clisvr.udp_port_svr";
const std::string PROP_UDP_PORT_SVR_DEFAULT = "31850";

const std::string PROP_CLI_HOSTNAME = "rocksdb-clisvr.client_hostname";
const std::string PROP_CLI_HOSTNAME_DEFAULT = "localhost";

const std::string PROP_SVR_HOSTNAME = "rocksdb-clisvr.server_hostname";
const std::string PROP_SVR_HOSTNAME_DEFAULT = "localhost";

const std::string PROP_MSG_SIZE = "rocksdb-clisvr.msg_size";
const std::string PROP_MSG_SIZE_DEFAULT = "1024";

const std::string PROP_NAME = "rocksdb.dbname";
const std::string PROP_NAME_DEFAULT = "";

const std::string PROP_FORMAT = "rocksdb.format";
const std::string PROP_FORMAT_DEFAULT = "single";

const std::string PROP_MERGEUPDATE = "rocksdb.mergeupdate";
const std::string PROP_MERGEUPDATE_DEFAULT = "false";

const std::string PROP_DESTROY = "rocksdb.destroy";
const std::string PROP_DESTROY_DEFAULT = "false";

const std::string PROP_COMPRESSION = "rocksdb.compression";
const std::string PROP_COMPRESSION_DEFAULT = "no";

const std::string PROP_MAX_BG_JOBS = "rocksdb.max_background_jobs";
const std::string PROP_MAX_BG_JOBS_DEFAULT = "0";

const std::string PROP_TARGET_FILE_SIZE_BASE = "rocksdb.target_file_size_base";
const std::string PROP_TARGET_FILE_SIZE_BASE_DEFAULT = "0";

const std::string PROP_TARGET_FILE_SIZE_MULT = "rocksdb.target_file_size_multiplier";
const std::string PROP_TARGET_FILE_SIZE_MULT_DEFAULT = "0";

const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE = "rocksdb.max_bytes_for_level_base";
const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT = "0";

const std::string PROP_WRITE_BUFFER_SIZE = "rocksdb.write_buffer_size";
const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

const std::string PROP_MAX_WRITE_BUFFER = "rocksdb.max_write_buffer_number";
const std::string PROP_MAX_WRITE_BUFFER_DEFAULT = "0";

const std::string PROP_COMPACTION_PRI = "rocksdb.compaction_pri";
const std::string PROP_COMPACTION_PRI_DEFAULT = "-1";

const std::string PROP_MAX_OPEN_FILES = "rocksdb.max_open_files";
const std::string PROP_MAX_OPEN_FILES_DEFAULT = "-1";

const std::string PROP_L0_COMPACTION_TRIGGER = "rocksdb.level0_file_num_compaction_trigger";
const std::string PROP_L0_COMPACTION_TRIGGER_DEFAULT = "0";

const std::string PROP_L0_SLOWDOWN_TRIGGER = "rocksdb.level0_slowdown_writes_trigger";
const std::string PROP_L0_SLOWDOWN_TRIGGER_DEFAULT = "0";

const std::string PROP_L0_STOP_TRIGGER = "rocksdb.level0_stop_writes_trigger";
const std::string PROP_L0_STOP_TRIGGER_DEFAULT = "0";

const std::string PROP_USE_DIRECT_WRITE = "rocksdb.use_direct_io_for_flush_compaction";
const std::string PROP_USE_DIRECT_WRITE_DEFAULT = "false";

const std::string PROP_USE_DIRECT_READ = "rocksdb.use_direct_reads";
const std::string PROP_USE_DIRECT_READ_DEFAULT = "false";

const std::string PROP_USE_MMAP_WRITE = "rocksdb.allow_mmap_writes";
const std::string PROP_USE_MMAP_WRITE_DEFAULT = "false";

const std::string PROP_USE_MMAP_READ = "rocksdb.allow_mmap_reads";
const std::string PROP_USE_MMAP_READ_DEFAULT = "false";

const std::string PROP_CACHE_SIZE = "rocksdb.cache_size";
const std::string PROP_CACHE_SIZE_DEFAULT = "0";

const std::string PROP_COMPRESSED_CACHE_SIZE = "rocksdb.compressed_cache_size";
const std::string PROP_COMPRESSED_CACHE_SIZE_DEFAULT = "0";

const std::string PROP_BLOOM_BITS = "rocksdb.bloom_bits";
const std::string PROP_BLOOM_BITS_DEFAULT = "0";

const std::string PROP_INCREASE_PARALLELISM = "rocksdb.increase_parallelism";
const std::string PROP_INCREASE_PARALLELISM_DEFAULT = "false";

const std::string PROP_OPTIMIZE_LEVELCOMP = "rocksdb.optimize_level_style_compaction";
const std::string PROP_OPTIMIZE_LEVELCOMP_DEFAULT = "false";

const std::string PROP_OPTIONS_FILE = "rocksdb.optionsfile";
const std::string PROP_OPTIONS_FILE_DEFAULT = "";

const std::string PROP_ENV_URI = "rocksdb.env_uri";
const std::string PROP_ENV_URI_DEFAULT = "";

const std::string PROP_FS_URI = "rocksdb.fs_uri";
const std::string PROP_FS_URI_DEFAULT = "";

#define READ_REQ 0
#define PUT_REQ 1
#define DELETE_REQ 2
