#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <unistd.h>
#include <cassert>
#include <string>
#include <msgpack.hpp>
#include <vector>


enum class Command {GET=0, WRITE=1};
//enum class Status {OK, NotFound, Corruption, NotSupported, InvalidArgument, IOError};

struct Status {
    enum Code {
      OK = 0,
      NotFound = 1,
      Corruption = 2,
      NotSupported = 3,
      InvalidArgument = 4,
      IOError = 5
    };
    union Code_t {
        Code code;
        int codeint;
    } status;
    std::string reason;
    MSGPACK_DEFINE(status.codeint, reason)
};


struct GetRequest{
    bool verify_checksums;  // Default: false
    bool fill_cache;   // Default: true
    std::string key;
    MSGPACK_DEFINE(verify_checksums, fill_cache, key)
};

struct GetResponse{
    Status status;
    std::string value;
    MSGPACK_DEFINE(status, value)
};

struct WriteOperation{
    bool do_delete;
    std::string key;
    std::string value;
    MSGPACK_DEFINE(do_delete, key, value)
};

struct WriteRequest{
    bool sync;
    std::vector<WriteOperation> operations;
    MSGPACK_DEFINE(sync, operations)
};

struct WriteResponse{
    Status status;
    MSGPACK_DEFINE(status)
};


#endif // PROTOCOL_H
