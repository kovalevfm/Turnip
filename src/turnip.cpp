#include <string>
#include <zmq.h>
#include <thread>
#include <sstream>
#include <msgpack.hpp>
#include <stdexcept>
#include <fstream>
#include <leveldb/env.h>
#include <iostream>
#include <libconfig.h++>
#include <cstdio>

#include "options.h"
#include "protocol.h"
#include "ldb.h"


void load_message(void *socket, std::string* buffer){
    buffer->clear();
    int ret;
    int more;
    size_t more_size = sizeof (more);
    do
    {
        zmq_msg_t part;
        ret = zmq_msg_init (&part);
        if (ret != 0){ throw std::ios_base::failure("can't init message"); }
        ret = zmq_msg_recv (&part, socket, 0);
        if (ret == -1){
            zmq_msg_close (&part);
            throw std::ios_base::failure("socket read error");
        }
        buffer->append((char*)zmq_msg_data(&part), zmq_msg_size(&part));
        ret = zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (ret == -1){
            zmq_msg_close (&part);
            throw std::ios_base::failure("can't get socket options");
        }
        zmq_msg_close (&part);
    }  while (more);
}


void process_message(msgpack::sbuffer* sbuf, std::string* buffer, LDB* db){
        sbuf->clear();
        msgpack::unpacked msg;
        msgpack::unpack(&msg, buffer->data(), buffer->size());
        msgpack::object obj = msg.get();
        if(obj.type != msgpack::type::ARRAY) { throw msgpack::type_error(); }
        msgpack::object* p = obj.via.array.ptr;
        if (p->type != msgpack::type::POSITIVE_INTEGER) { throw msgpack::type_error(); }
        if (p->via.i64 == (int)Command::GET){
            ++p;
            GetResponse response;
            db->Get(p->as<GetRequest>(), &response);
            msgpack::pack(sbuf, response);
        } else if (p->via.i64 == (int)Command::WRITE){
            ++p;
            // WriteRequest request;
            // p->convert(&request);
            WriteResponse response;
            db->Write(p->as<WriteRequest>(), &response);
            msgpack::pack(sbuf, response);
        }
}

void worker_routine (void *context, LDB* db)
{
    void *socket = zmq_socket (context, ZMQ_REP);
    std::string buffer;
    msgpack::sbuffer sbuf;
    zmq_connect (socket, "inproc://workers");
    while (true) {
        try{
            load_message(socket, &buffer);
            try{
                process_message(&sbuf, &buffer, db);
            } catch (const msgpack::type_error& e) {
                WriteResponse response;
                response.status.status.code = Status::InvalidArgument;
                response.status.reason = "can't parse message, invalid protocol";
                sbuf.clear();
                msgpack::pack(sbuf, response);
            }
            int rc = zmq_send (socket, sbuf.data(), sbuf.size(), 0);
            if(rc != (int)sbuf.size()) { std::ios_base::failure("socket write error"); }
        } catch (const std::ios_base::failure& e) {
            leveldb::Log(db->getLogger(), "socket error %s", e.what());
        }
    }
    return;
}

int main (int argc, char *argv[])
{
    Options opt;
    try{
        FILE* f;
        if (argc > 1){
            f = fopen(argv[1], "r");
        } else {
            f = tmpfile ();
        }
        opt = loadOptions(f);
        fclose(f);
    } catch (const libconfig::ParseException& e){
        std::cerr<<"can't parse settings ";
        if (argc > 1){std::cerr<<argv[1];}
        std::cerr<<" error "<<e.what()<<std::endl;
        return -1;
    }
    LDB db(opt);
    std::ostringstream oss;
    oss << "tcp://*:" << opt.port;

    void *context = zmq_ctx_new ();

    //  Socket to talk to clients
    void *clients = zmq_socket (context, ZMQ_ROUTER);
    zmq_bind (clients, oss.str().c_str());

    //  Socket to talk to workers
    void *workers = zmq_socket (context, ZMQ_DEALER);
    zmq_bind (workers, "inproc://workers");

    //  Launch pool of worker threads
    std::vector<std::unique_ptr<std::thread> > threads_vec;
    for (int thread_nbr = 0; thread_nbr < opt.threads_num; thread_nbr++) {
        std::thread* tmp =new std::thread(worker_routine, context, &db);
        threads_vec.push_back(std::unique_ptr<std::thread>(tmp));
    }
    //  Connect work threads to client threads via a queue
    zmq_proxy (clients, workers, NULL);
    for (auto it = threads_vec.begin() ; it != threads_vec.end() ; ++ it){
        (*it)->join();
    }
    //  We never get here, but clean up anyhow
    zmq_close (clients);
    zmq_close (workers);
    zmq_ctx_destroy (context);
    return 0;
}


