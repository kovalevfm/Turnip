#include "transport.h"
#include <zmq.h>
#include <leveldb/env.h>

#include <sstream>
#include <iostream>


Transport::Transport(InQueue* q_in_,  OutQueue* q_out_, leveldb::Logger* logger_)
    : packer(&buffer)
    , logger(logger_)
    , q_in(q_in_)
    , q_out(q_out_){
}
bool Transport::recv_next(Message* message){
    msgpack::unpacked result;
    while (unpacker.next(&result)) {
        message->messsage = result.get();
        message->zone = std::unique_ptr<msgpack::zone>(result.zone().release());
        return true;
    }
    return false;
}
void Transport::commit_message(){
    IdMesage res(identity, std::string(buffer.data(), buffer.size()));
    q_out->block_push(res);
    buffer.clear();
}

void Transport::load_message(){
    IdMesage msg = q_in->block_pop();
    identity = msg.first;
    unpacker.reserve_buffer(msg.second.size());
    memcpy(unpacker.buffer(), msg.second.data(), msg.second.size());
    unpacker.buffer_consumed(msg.second.size());
}

leveldb::WriteOptions WriteOptions::get_leveldb_options(){
    leveldb::WriteOptions result;
    result.sync = sync;
    return result;
}

leveldb::ReadOptions ReadOptions::get_leveldb_options(){
    leveldb::ReadOptions result;
    result.fill_cache = fill_cache;
    result.verify_checksums = verify_checksums;
    return result;
}

Status::Status(const leveldb::Status &ldb_status){
    if (ldb_status.ok()){
        code = (int)StatusCode::OK;
    } else if (ldb_status.IsNotFound()){
        code = (int)StatusCode::NotFound;
    } else{
        code = (int)StatusCode::Corruption;
        reason = "unknown code";
    }
}

RangeValue::RangeValue(leveldb::Iterator *it){
    status = it->status();
    key.assign(it->key().data(), it->key().size());
    value.assign(it->value().data(), it->value().size());
}
