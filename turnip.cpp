/*
    Multithreaded Hello World server in C
*/

#include <unistd.h>
#include <cassert>
#include <string>
#include <iostream>
#include <zmq.hpp>
#include <thread>
#include <sstream>
#include <msgpack.hpp>
#include <vector>
#include <fstream>

#include "options.h"
#include "protocol.h"
#include "ldb.h"

void worker_routine (zmq::context_t *context, LDB* db)
{
    zmq::socket_t socket (*context, ZMQ_REP);
    socket.connect ("inproc://workers");
    while (true) {
        //  Wait for next request from client
        zmq::message_t request;
        socket.recv (&request);

        msgpack::sbuffer sbuf;
        msgpack::unpacked msg;
        msgpack::unpack(&msg, (char*)request.data(), request.size());
        msgpack::object obj = msg.get();
        if(obj.type != msgpack::type::ARRAY) { throw msgpack::type_error(); }
        msgpack::object* p = obj.via.array.ptr;
        if (p->type != msgpack::type::POSITIVE_INTEGER) { throw msgpack::type_error(); }
        if (p->via.i64 == (int)Command::GET){
            ++p;
            GetRequest request;
            p->convert(&request);
            GetResponse response;
            db->Get(request, &response);
            msgpack::pack(sbuf, response);
        } else if (p->via.i64 == (int)Command::WRITE){
            ++p;
            WriteRequest request;
            p->convert(&request);
            WriteResponse response;
            db->Write(request, &response);
            msgpack::pack(sbuf, response);
        }
        zmq::message_t reply (sbuf.size());
        memcpy ((void *) reply.data (), sbuf.data(), sbuf.size());
        socket.send (reply);
    }
    return;
}


int main (int argc, char *argv[])
{
    Options opt;
    if (argc > 1){
        opt = loadOptions(argv[1]);
    } else{
        std::istringstream iss ("{}");
        opt = loadOptions(iss);
    }
    LDB db(opt);
    std::ostringstream oss;
    oss << "tcp://*:" << opt.port;

    //  Prepare our context and sockets
    zmq::context_t context (1);
    zmq::socket_t clients (context, ZMQ_ROUTER);
    clients.bind (oss.str().c_str());
    zmq::socket_t workers (context, ZMQ_DEALER);
    workers.bind ("inproc://workers");

    //  Launch pool of worker threads
    std::vector<std::unique_ptr<std::thread> > threads_vec;
    for (int thread_nbr = 0; thread_nbr < opt.threads_num; thread_nbr++) {
        std::thread* tmp =new std::thread(worker_routine, &context, &db);
        threads_vec.push_back(std::unique_ptr<std::thread>(tmp));
    }
    //  Connect work threads to client threads via a queue
    zmq::proxy (clients, workers, NULL);
    for (auto it = threads_vec.begin() ; it != threads_vec.end() ; ++ it){
        (*it)->join();
    }
    return 0;
}
