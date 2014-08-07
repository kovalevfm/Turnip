#include "options.h"
#include <jsoncpp/json/json.h>
#include <fstream>
#include <exception>
#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>

Options loadOptions(const std::string& path){
    std::ifstream ifs;
    ifs.open (path, std::ifstream::in);
    return loadOptions(ifs);
}

Options loadOptions(std::istream& source){
    Options result;
    Json::Value root;   // will contains the root value after parsing.
    Json::Reader reader;
    bool parsingSuccessful = reader.parse( source, root );
    if ( !parsingSuccessful )
    {
        throw std::invalid_argument("failed to parse settings");
    }
    result.db_path = root.get("path", "db" ).asString();
    result.port = root.get("port", 5544 ).asInt();
    result.threads_num = root.get("threads", 4 ).asInt();
    result.ldb_options.create_if_missing = root.get("create_if_missing", true ).asBool();
    result.ldb_options.error_if_exists = root.get("error_if_exists", false ).asBool();
    result.ldb_options.paranoid_checks = root.get("paranoid_checks", false ).asBool();
    result.ldb_options.write_buffer_size = root.get("write_buffer_size", 4194304 ).asInt();
    result.ldb_options.max_open_files = root.get("max_open_files", 1000 ).asInt();
    result.ldb_options.block_size = root.get("block_size", 4096 ).asInt();
    result.ldb_options.block_restart_interval = root.get("block_restart_interval", 16 ).asInt();
    bool comression = root.get("compression", true ).asBool();
    int bloom_filter_cnt = root.get("bloom_filter_cnt", 10 ).asInt();
    int block_cache_size = root.get("block_cache_size", 8388608 ).asInt();

    if (bloom_filter_cnt > 0){
        result.ldb_options.filter_policy = leveldb::NewBloomFilterPolicy(bloom_filter_cnt);
    }
    if (comression){
        result.ldb_options.compression = leveldb::kSnappyCompression;
    }
    if (block_cache_size > 0){
        result.ldb_options.block_cache = leveldb::NewLRUCache(block_cache_size);
    }
    return result;
}


