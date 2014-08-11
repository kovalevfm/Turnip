#ifndef OPTIONS_H
#define OPTIONS_H

#include <string>
#include <iostream>
#include <cstdio>
#include <leveldb/options.h>

struct Options{
    std::string db_path;
    int threads_num;
    int port;
    leveldb::Options ldb_options;
};

Options loadOptions(FILE* f);

#endif // OPTIONS_H
