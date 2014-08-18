#include <string>
#include <zmq.h>
#include <thread>
#include <sstream>
#include <stdexcept>
#include <leveldb/env.h>
#include <iostream>
#include <utility>

#include "options.h"
#include "transport.h"
#include "ldb.h"
#include "queue.h"
#include <poll.h>
#include "zmqwrapper.h"


#include <thread>             // std::thread, std::this_thread::yield std::this_thread::sleep_for
#include <mutex>              // std::mutex, std::unique_lock
#include <condition_variable> // std::condition_variable
#include <chrono>         // std::chrono::seconds


void worker_routine (void *context, LDB* db, leveldb::Logger* logger)
{
    Transport t(context, logger);
    Message m;
    while (true){
        t.recv_next(&m);
//        leveldb::Log(logger, "write to batch %d", m.messsage.as<int>());
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



//C bar;
//std::thread(&C::increase_member,std::ref(bar),1000)


void wait_send(Queue<std::pair<std::string, std::string> >* q_in,  Queue<std::pair<std::string, std::string> >* q_out, const std::string& name){
//    pollfd pfd;
//    pfd.fd = q_in->get_fd();
//    pfd.events = POLLIN;
//    while (true){
//        poll(&pfd, 1, -1);
//        usleep(1000000);
//        q_out->push(q_in->front());
//        q_in->pop();
//    }
    while (true){
        std::pair<std::string, std::string> res = q_in->block_pop();
        res.second += name;
//        std::cout<<res.second<<std::endl;
        q_out->push(res);
    }
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
    Queue<std::pair<std::string, std::string> > q_in;
    Queue<std::pair<std::string, std::string> > q_out;
    ZMQWrapper zmq_server(opt.get_port(), q_out.get_fd());
    std::string buff_identity;
    std::string buff_message;
    std::thread t1(wait_send, &q_in, &q_out, "1");
    std::thread t2(wait_send, &q_in, &q_out, "2");
//                  std::make_pair(buff_identity, buff_message), &q);

    while (true){
        zmq_server.wait();
        if (zmq_server.recv_next(&buff_identity, &buff_message)){
            q_in.push(std::make_pair(buff_identity, buff_message));
        }
        if (!q_out.empty()){
            if ( zmq_server.send_next(&q_out.front().first, &q_out.front().second)){
                q_out.pop();
            }
        }
//        if (zmq_server.send_next(&buff_identity, &buff_message)){

//        }
    }

    //            std::cout<<"recv form "<<buff_identity<<" msg "<<buff_message<<std::endl;

    //            std::cout<<"thread"<<std::endl;
    //            bool st = zmq_server.send_next(&buff_identity, &buff_message);
    //            std::cout<<" .. send back "<<st<<std::endl;

//    std::ostringstream oss;
//    oss << "tcp://*:" << opt.get_port();

//    void *context = zmq_ctx_new ();

//    //  Socket to talk to clients
//    void *clients = zmq_socket (context, ZMQ_ROUTER);

//    zmq_setsockopt(clients, ZMQ_SNDHWM, &HWM, sizeof(int));
//    zmq_setsockopt(clients, ZMQ_RCVHWM, &HWM, sizeof(int));
//    zmq_setsockopt(clients, ZMQ_ROUTER_MANDATORY, &MANDATORY, sizeof(int));
//    zmq_bind (clients, oss.str().c_str());






//    void *workers = zmq_socket (context, ZMQ_DEALER);

//    zmq_setsockopt(clients, ZMQ_SNDHWM, &HWM, sizeof(int));
//    zmq_setsockopt(clients, ZMQ_RCVHWM, &HWM, sizeof(int));
//    zmq_setsockopt(workers, ZMQ_SNDHWM, &HWM, sizeof(int));
//    zmq_setsockopt(workers, ZMQ_RCVHWM, &HWM, sizeof(int));
//    zmq_bind (clients, oss.str().c_str());
//    zmq_bind (workers, "inproc://workers");

//    //  Launch pool of worker threads
//    std::vector<std::unique_ptr<std::thread> > threads_vec;
//    for (int thread_nbr = 0; thread_nbr < opt.get_threads_num(); thread_nbr++) {
//        std::thread* tmp =new std::thread(worker_routine, context, &db, opt.get_logger());
//        threads_vec.push_back(std::unique_ptr<std::thread>(tmp));
//    }
//    //  Connect work threads to client threads via a queue
//    zmq_proxy (clients, workers, NULL);
//    for (auto it = threads_vec.begin() ; it != threads_vec.end() ; ++ it){
//        (*it)->join();
//    }
//    //  We never get here, but clean up anyhow
//    zmq_close (clients);
//    zmq_close (workers);
//    zmq_ctx_destroy (context);
    return 0;
}


