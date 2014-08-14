#include <string>
#include <zmq.h>
#include <thread>
#include <sstream>
#include <stdexcept>
#include <leveldb/env.h>
#include <iostream>

#include "options.h"
#include "transport.h"
#include "ldb.h"


void worker_routine (void *context, LDB* db, leveldb::Logger* logger)
{
    Transport t(context, logger);
    Message m;
    while (true){
        t.recv_next(&m);
        try{
            switch(m.messsage.as<int>()){
            case (int)Command::GET:
                db->Get(&t);
                break;
            case (int)Command::WRITE:
                db->Write(&t);
                break;
            case (int)Command::RANGE:
                db->Range(&t);
                break;
            }
        } catch (const std::bad_cast& e){
            t.read_tail();
            t.send_next(Status(StatusCode::InvalidArgument, e.what()));
            leveldb::Log(logger, "format error %s", e.what());
        } catch (const std::ios_base::failure& e){
            t.read_tail();
            t.send_next(Status(StatusCode::IOError, e.what()));
            leveldb::Log(logger, "socet error %s", e.what());
        }
        t.commit_message();
    };
    return;
}


int main (int argc, char *argv[])
{
    std::string fname;
    if (argc > 1){
        fname = argv[1];
    }
    Options opt;
    if (!opt.Load(fname)){return -1;}
    LDB db(opt);
    std::ostringstream oss;
    oss << "tcp://*:" << opt.get_port();

    void *context = zmq_ctx_new ();

    //  Socket to talk to clients
    void *clients = zmq_socket (context, ZMQ_ROUTER);


    //  Socket to talk to workers
    void *workers = zmq_socket (context, ZMQ_DEALER);
    int cnt = 10000000;
    zmq_setsockopt(clients, ZMQ_SNDHWM, &cnt, sizeof(int));
    zmq_setsockopt(clients, ZMQ_RCVHWM, &cnt, sizeof(int));
    zmq_setsockopt(workers, ZMQ_SNDHWM, &cnt, sizeof(int));
    zmq_setsockopt(workers, ZMQ_RCVHWM, &cnt, sizeof(int));
    zmq_bind (clients, oss.str().c_str());
    zmq_bind (workers, "inproc://workers");

    //  Launch pool of worker threads
    std::vector<std::unique_ptr<std::thread> > threads_vec;
    for (int thread_nbr = 0; thread_nbr < opt.get_threads_num(); thread_nbr++) {
        std::thread* tmp =new std::thread(worker_routine, context, &db, opt.get_logger());
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


