#ifndef LDB_H
#define LDB_H

#include <leveldb/db.h>

#include "options.h"
#include "protocol.h"

class LDB{
public:
    LDB(const Options& options);
    virtual ~LDB();

    void Get(const GetRequest& request, GetResponse* response);
    void Write(const WriteRequest& request, WriteResponse* response);

private:
    leveldb::DB* db;
    leveldb::Cache* cache;
    const leveldb::FilterPolicy* filter_policy;
};

#endif // LDB_H
