#include "transport.h"
#include <zmq.h>

#include <iostream>

Transport::Transport(void *context)
    : socket(zmq_socket (context, ZMQ_REP))
    , more(1)
    , writer(new Writer(socket))
    , unpacker(new msgpack::unpacker())
    , packer(new msgpack::packer<Writer>(*writer))
{
    zmq_connect (socket, "inproc://workers");
}

Message Transport::recv_next()
{
    size_t more_size = sizeof (more);
    msgpack::unpacked result;
    while(true)
    {
        while (unpacker->next(&result)) {
            return Message(result.zone().release(), result.get());
        }
        if (more == 0){
            more = 1;
            return Message(NULL, msgpack::object());
        }
        zmq_msg_t part;
        int ret = zmq_msg_init (&part);
        if (ret != 0){ throw network_error("can't init message"); }
        ret = zmq_msg_recv (&part, socket, 0);
        if (ret == -1){
            zmq_msg_close (&part);
            throw network_error("socket read error");
        }
        unpacker->reserve_buffer(zmq_msg_size(&part));
        memcpy(unpacker->buffer(), zmq_msg_data(&part), zmq_msg_size(&part));
        unpacker->buffer_consumed(zmq_msg_size(&part));
        ret = zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (ret == -1){
            zmq_msg_close (&part);
            throw network_error("can't get socket options");
        }
        zmq_msg_close (&part);
    };
}


void Transport::commit_message()
{
    writer->commit();
}


Transport::Writer::Writer(void *socket_) :socket(socket_){}

void Transport::Writer::write(const char *buf, size_t buflen)
{
    int rc = zmq_send (socket, buf, buflen, ZMQ_SNDMORE);
    if (rc != (int)buflen){
        throw network_error("can't send message");
    }
}

void Transport::Writer::commit()
{
    int rc = zmq_send (socket, NULL, 0, 0);
    if (rc != 0){
        throw network_error("can't commit message");
    }

}

void Transport::read_tail(){
    int event;
    size_t event_size = sizeof (event);
    int rc = zmq_getsockopt (socket, ZMQ_EVENTS, &event, &event_size);
    assert (rc == 0);
    while (event & ZMQ_POLLIN){
        recv_next();
        rc = zmq_getsockopt (socket, ZMQ_EVENTS, &event, &event_size);
        assert (rc == 0);
    }
    if (more == 0){
        recv_next();
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
