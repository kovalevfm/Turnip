#ifndef TRANSPORT_H
#define TRANSPORT_H
#include <msgpack.hpp>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <leveldb/iterator.h>
#include <stdexcept>

#include "queue.h"

enum class Command {END=0, GET=1, WRITE=2, RANGE=3};
enum class StatusCode {OK=0, NotFound=1, Corruption=2, NotSupported=3, InvalidArgument=4, IOError=5};

class format_error : public std::bad_cast {
public:
    format_error(const std::string& what_) : std::bad_cast(), msg(what_){}
    virtual const char* what() const throw (){
        return msg.c_str();
    }
    virtual ~format_error() throw(){}
private:
    std::string msg;
};


struct WriteOptions{
    WriteOptions() : sync(false) {}
    bool sync; //Default: false
    leveldb::WriteOptions get_leveldb_options();
    MSGPACK_DEFINE(sync)
};

struct ReadOptions{
    ReadOptions() : verify_checksums(false), fill_cache(true) {}
    bool verify_checksums;  // Default: false
    bool fill_cache;   // Default: true
    leveldb::ReadOptions get_leveldb_options();
    MSGPACK_DEFINE(verify_checksums, fill_cache)
};


struct Message{
     std::unique_ptr<msgpack::zone> zone;
     msgpack::object messsage;
     Message() : zone(), messsage() {}
     Message(msgpack::zone* zone_, const msgpack::object& messsage_):
        zone(zone_), messsage(messsage_){}
};


struct Status {
    int code;
    Status() : code((int)StatusCode::OK){}
    Status(StatusCode code_, const std::string msg) : code((int)code_), reason(msg) {}
    Status(const leveldb::Status& ldb_status);
    std::string reason;
    MSGPACK_DEFINE(code, reason)
};


struct WriteOperation{
    bool do_delete;
    std::string key;
    std::string value;
    MSGPACK_DEFINE(do_delete, key, value)
};

struct RangeValue{
    RangeValue(leveldb::Iterator* it);
    Status status;
    std::string key;
    std::string value;
    MSGPACK_DEFINE(status, key, value)
};

typedef std::pair<std::string, std::string> IdMesage;
typedef Queue<IdMesage> InQueue;
typedef FDQueue<IdMesage> OutQueue;

class Transport
{
public:
    Transport(InQueue* q_in_,  OutQueue* q_out_, leveldb::Logger* logger_);
    void load_message();
    bool recv_next(Message* message);
    template <typename T> void send_message(const T& v);
    template <typename T> void send_part(const T& v);
    void commit_message();

private:
    msgpack::sbuffer buffer;
    msgpack::unpacker unpacker;
    msgpack::packer<msgpack::sbuffer> packer;
    leveldb::Logger* logger;
    std::string identity;
    InQueue* q_in;
    OutQueue* q_out;
};

template <typename T> void Transport::send_part(const T &v)
{
    packer.pack(v);
}

template <typename T> void Transport::send_message(const T &v)
{
    send_part(v);
    commit_message();
}



#endif // TRANSPORT_H


