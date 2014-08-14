#ifndef LDB_H
#define LDB_H

#include <leveldb/db.h>
#include <leveldb/env.h>

#include "options.h"
#include "transport.h"

class LDB{
public:
    LDB(Options& options_);
    virtual ~LDB();

    void Get(Transport* t);
    void Write(Transport* t);
    void Range(Transport* t);

private:
    leveldb::DB* db;
    leveldb::Logger* logger;
};

#endif // LDB_H
