#include "transport.h"
#include <zmq.h>
#include <leveldb/env.h>

#include <sstream>
#include <iostream>

inline std::string string_to_hex(const std::string& input){
    static const char* const lut = "0123456789abcdef";
    size_t len = input.length();
    std::string output;
    output.reserve(2 * len);
    for (size_t i = 0; i < len; ++i)    {
        const unsigned char c = input[i];
        output.push_back(lut[c >> 4]);
        output.push_back(lut[c & 15]);
    }
    return output;
}

Transport::Transport(void *context, leveldb::Logger *logger_)
    : socket(zmq_socket (context, ZMQ_DEALER))
    , writer(new Writer(socket, logger_, &last_identity))
    , unpacker(new msgpack::unpacker())
    , packer(new msgpack::packer<Writer>(*writer))
    , logger(logger_)
    , state(TransportState::RECIVE)
    , more(0)
{
    zmq_connect (socket, "inproc://workers");
}

bool Transport::recv_next(Message* message)
{
    if (state == TransportState::SEND){
        return false;
    }
    state = TransportState::RECIVE;
    size_t more_size = sizeof (more);
    msgpack::unpacked result;
    while(true)
    {
        while (unpacker->next(&result)) {
            message->messsage = result.get();
            message->zone = std::unique_ptr<msgpack::zone>(result.zone().release());
            if (message->messsage.type == msgpack::type::POSITIVE_INTEGER && message->messsage.as<int>() == (int)Command::END){
                state = TransportState::SEND;
//                leveldb::Log(logger, "get END");
                return false;
            }
            std::ostringstream oss;
            oss << message->messsage;
            return true;
        }
        zmq_msg_t part;
        int ret = zmq_msg_init (&part);
        if (ret != 0){ throw network_error("can't init message"); }
        ret = zmq_msg_recv (&part, socket, 0);
        if (ret == -1){
            zmq_msg_close (&part);
            throw network_error("socket read error");
        }
        if (more == 0){
            last_identity.assign((char*)zmq_msg_data(&part), zmq_msg_size(&part));
        } else {
            unpacker->reserve_buffer(zmq_msg_size(&part));
            memcpy(unpacker->buffer(), zmq_msg_data(&part), zmq_msg_size(&part));
            unpacker->buffer_consumed(zmq_msg_size(&part));
        }
        ret = zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (ret == -1){
            zmq_msg_close (&part);
            throw network_error("can't get socket options");
        }
        zmq_msg_close (&part);
    };
}


Transport::Writer::Writer(void *socket_, leveldb::Logger* logger_, std::string* last_identity_) :socket(socket_), logger(logger_), last_identity(last_identity_){}

void Transport::Writer::write(const char *buf, size_t buflen)
{
    if (buflen == 0){
        return;
    }
    leveldb::Log(logger, "sent to %s -> %s", string_to_hex(*last_identity).c_str(), (buflen<10) ? string_to_hex(std::string(buf, buflen)).c_str() : std::string(buf, buflen).c_str());
    int rc = zmq_send (socket, last_identity->data(), last_identity->size(), ZMQ_SNDMORE);
    if (rc != (int)last_identity->size()){
        throw network_error("can't send message");
    }
    rc = zmq_send (socket, buf, buflen, 0);
    if (rc != (int)buflen){
        throw network_error("can't send message");
    }
}


void Transport::commit_message()
{
    send_next((int)Command::END);
    state = TransportState::READY;
}

void Transport::read_tail(){
    Message m;
    while (state == TransportState::RECIVE){
        recv_next(&m);
    }
}


leveldb::WriteOptions WriteOptions::get_leveldb_options()
{
    leveldb::WriteOptions result;
    result.sync = sync;
    return result;
}


leveldb::ReadOptions ReadOptions::get_leveldb_options()
{
    leveldb::ReadOptions result;
    result.fill_cache = fill_cache;
    result.verify_checksums = verify_checksums;
    return result;
}


Status::Status(const leveldb::Status &ldb_status)
{
    if (ldb_status.ok()){
        code = (int)StatusCode::OK;
    } else if (ldb_status.IsNotFound()){
        code = (int)StatusCode::NotFound;
    } else{
        code = (int)StatusCode::Corruption;
        reason = "unknown code";
    }
}



RangeValue::RangeValue(leveldb::Iterator *it)
{
    status = it->status();
    key.assign(it->key().data(), it->key().size());
    value.assign(it->value().data(), it->value().size());
}
