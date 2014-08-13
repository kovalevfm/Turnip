#ifndef TRANSPORT_H
#define TRANSPORT_H
#include <msgpack.hpp>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <leveldb/iterator.h>
#include <stdexcept>

enum class Command {GET=0, WRITE=1, RANGE=2};
enum class StatusCode {OK=0, NotFound=1, Corruption=2, NotSupported=3, InvalidArgument=4, IOError=5};

class format_error : public std::bad_cast {
public:
    format_error(const std::string& what_) : std::bad_cast(), msg(what_){}
    virtual const char* what() const noexcept{
        return msg.c_str();
    }
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

class Transport
{
public:
    class Writer{
    public:
        Writer(void *socket_);
        void write(const char* buf, size_t buflen);
        void commit();
    private:
        void *socket;
        bool do_commit;
    };

    Transport(void *context);
    Message recv_next();
    template <typename T> void send_next(const T& v);
    void commit_message();
    void read_tail();

private:
    void *socket;
    int more;
    std::unique_ptr<Writer> writer;
    std::unique_ptr<msgpack::unpacker> unpacker;
    std::unique_ptr<msgpack::packer<Writer> > packer;
};

template <typename T> void Transport::send_next(const T &v)
{
    packer->pack(v);
}

#endif // TRANSPORT_H


