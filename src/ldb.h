#ifndef LDB_H
#define LDB_H

#include <leveldb/db.h>
#include <leveldb/env.h>

#include "options.h"
#include "protocol.h"

class LDB{
public:
    LDB(Options& options_);
    virtual ~LDB();

    void Get(const GetRequest& request, GetResponse* response);
    void Write(const WriteRequest& request, WriteResponse* response);
    leveldb::Logger* getLogger();

private:
    leveldb::DB* db;
    Options options;
    // leveldb::Cache* cache;
    // const leveldb::FilterPolicy* filter_policy;
};

#endif // LDB_H
