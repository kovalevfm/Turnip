#include "options.h"
#include <libconfig.h++>
#include <exception>
#include <stdexcept>
#include <leveldb/cache.h>
#include <leveldb/env.h>
// #include <leveldb/filter_policy.h>


bool Options::Load(const std::string& fname){
    FILE* f;
    if (fname.size() > 0){
        f = fopen(fname.c_str(), "r");
    } else {
        f = tmpfile ();
    }
    try{
        libconfig::Config cfg;
        std::string log_file;
        bool compression;
        // int bloom_filter_cnt;
        int block_cache_size;
        int write_buffer_size;
        int block_size;
        cfg.read(f);
        if (!cfg.lookupValue("path", db_path)){db_path = "db";}
        if (!cfg.lookupValue("port", port)){port = 5544;}
        if (!cfg.lookupValue("threads", threads_num)){threads_num = 1;}
        if (!cfg.lookupValue("log_file", log_file)){log_file = "turnip.log";}
        if (!cfg.lookupValue("create_if_missing", ldb_options.create_if_missing)){ldb_options.create_if_missing = true;}
        if (!cfg.lookupValue("error_if_exists", ldb_options.error_if_exists)){ldb_options.error_if_exists = false;}
        if (!cfg.lookupValue("paranoid_checks", ldb_options.paranoid_checks)){ldb_options.paranoid_checks = false;}
        if (!cfg.lookupValue("write_buffer_size", write_buffer_size)){write_buffer_size = 4194304;}
        if (!cfg.lookupValue("max_open_files", ldb_options.max_open_files)){ldb_options.max_open_files = 1000;}
        if (!cfg.lookupValue("block_size", block_size)){block_size = 4096;}
        if (!cfg.lookupValue("block_restart_interval", ldb_options.block_restart_interval)){ldb_options.block_restart_interval = 16;}
        if (!cfg.lookupValue("compression", compression)){compression = false;}
        // if (!cfg.lookupValue("bloom_filter_cnt", bloom_filter_cnt)){bloom_filter_cnt = 10;}
        if (!cfg.lookupValue("block_cache_size", block_cache_size)){block_cache_size = 8388608;}
        if (compression){
            ldb_options.compression = leveldb::kSnappyCompression;
        }
        leveldb::Env* env = leveldb::Env::Default();
        env->NewLogger(log_file, &logger);
        ldb_options.info_log = logger;
        cache = leveldb::NewLRUCache(block_cache_size);
        ldb_options.block_cache = cache;
        ldb_options.block_size = block_size;
        ldb_options.write_buffer_size = write_buffer_size;
    } catch (const libconfig::ParseException& e){
        std::cerr<<"can't parse settings "<<fname<<" error "<<e.what()<<std::endl;
        return false;
    }
    fclose(f);
    return true;
}

Options::~Options()
{
    delete logger;
    delete cache;
}
