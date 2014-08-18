#ifndef OPTIONS_H
#define OPTIONS_H

#include <string>
#include <iostream>
#include <cstdio>
#include <leveldb/options.h>

class Options{
public:
    bool Load(const std::string& fname);
    leveldb::Options get_ldb_options(){
        return ldb_options;
    }
    const char* get_db_path(){
        return db_path.c_str();
    }
    int get_threads_num(){return threads_num;}
    int get_port(){return port;}
    leveldb::Logger* get_logger(){return logger;}
    virtual ~Options();

private:
    leveldb::Options ldb_options;
    std::string db_path;
    int threads_num;
    int port;
    leveldb::Logger* logger;
    leveldb::Cache* cache;

};


#endif // OPTIONS_H
