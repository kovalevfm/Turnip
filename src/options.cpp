#include "options.h"
#include <libconfig.h++>
#include <exception>
#include <stdexcept>
#include <leveldb/cache.h>
#include <leveldb/env.h>
// #include <leveldb/filter_policy.h>

Options loadOptions(FILE* f){
    libconfig::Config cfg;
    std::string log_file;
    bool compression;
    // int bloom_filter_cnt;
    int block_cache_size;
    int write_buffer_size;
    int block_size;
    Options result;
    cfg.read(f);
    if (!cfg.lookupValue("path", result.db_path)){result.db_path = "db";}
    if (!cfg.lookupValue("port", result.port)){result.port = 5544;}
    if (!cfg.lookupValue("threads", result.threads_num)){result.threads_num = 4;}
    if (!cfg.lookupValue("log_file", log_file)){log_file = "turnip.log";}
    if (!cfg.lookupValue("create_if_missing", result.ldb_options.create_if_missing)){result.ldb_options.create_if_missing = true;}
    if (!cfg.lookupValue("error_if_exists", result.ldb_options.error_if_exists)){result.ldb_options.error_if_exists = false;}
    if (!cfg.lookupValue("paranoid_checks", result.ldb_options.paranoid_checks)){result.ldb_options.paranoid_checks = false;}
    if (!cfg.lookupValue("write_buffer_size", write_buffer_size)){write_buffer_size = 4194304;}
    if (!cfg.lookupValue("max_open_files", result.ldb_options.max_open_files)){result.ldb_options.max_open_files = 1000;}
    if (!cfg.lookupValue("block_size", block_size)){block_size = 4096;}
    if (!cfg.lookupValue("block_restart_interval", result.ldb_options.block_restart_interval)){result.ldb_options.block_restart_interval = 16;}
    if (!cfg.lookupValue("compression", compression)){compression = false;}
    // if (!cfg.lookupValue("bloom_filter_cnt", bloom_filter_cnt)){bloom_filter_cnt = 10;}
    if (!cfg.lookupValue("block_cache_size", block_cache_size)){block_cache_size = 8388608;}
    if (compression){
        result.ldb_options.compression = leveldb::kSnappyCompression;
    }
    result.ldb_options.block_size = block_size;
    result.ldb_options.write_buffer_size = write_buffer_size;
    result.ldb_options.block_cache = leveldb::NewLRUCache(block_cache_size);
    leveldb::Env* env = leveldb::Env::Default();
    env->NewLogger(log_file, &result.ldb_options.info_log);
    return result;
}