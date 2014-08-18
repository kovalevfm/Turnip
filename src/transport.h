#ifndef TRANSPORT_H
#define TRANSPORT_H
#include <msgpack.hpp>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <leveldb/iterator.h>
#include <stdexcept>

#include "queue.h"

enum class Command {END=0, GET=1, WRITE=2, RANGE=3};
enum class TransportState {READY=0, RECIVE=1, SEND=2};
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

class network_error : public std::ios_base::failure {
public:
    network_error(const std::string& what) : std::ios_base::failure(what){}
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

//class Transport
//{
//public:
//    Transport(void *context, leveldb::Logger* logger_);
//    bool recv_next(Message* message);
//    template <typename T> void send_next(const T& v);
//    void commit_message();
//    void read_tail();
//    TransportState get_state(){return state;}

//private:
//    void write(const char* buf, size_t buflen);

//    void *socket;
//    std::string last_identity;
//    std::unique_ptr<msgpack::unpacker> unpacker;
//    leveldb::Logger* logger;
//    TransportState state;
//    int more;
//    msgpack::sbuffer buffer;
//};

//template <typename T> void Transport::send_next(const T &v)
//{
//    msgpack::pack(buffer, v);
//    write(buffer.data(), buffer.size());
//    buffer.clear();
//}


class Transport
{
public:
    Transport(Queue<std::pair<std::string, std::string> >* q_in_,  Queue<std::pair<std::string, std::string> >* q_out_, leveldb::Logger* logger_);
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
    Queue<std::pair<std::string, std::string> >* q_in;
    Queue<std::pair<std::string, std::string> >* q_out;
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


