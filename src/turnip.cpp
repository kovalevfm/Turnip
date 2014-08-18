#include <string>
#include <zmq.h>
#include <vector>
#include <sstream>
#include <stdexcept>
#include <leveldb/env.h>
#include <utility>
#include <thread>

#include "ldb.h"
#include "options.h"
#include "transport.h"
#include "queue.h"
#include "zmqwrapper.h"

#include <iostream>

void worker_routine(Queue<std::pair<std::string, std::string> >* q_in,  Queue<std::pair<std::string, std::string> >* q_out, LDB* db, leveldb::Logger* logger){
    Transport t(q_in, q_out, logger);
    Message m;
    while (true){
        t.load_message();
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
            t.send_message(Status(StatusCode::InvalidArgument, e.what()));
            leveldb::Log(logger, "format error %s", e.what());
        } catch (const std::ios_base::failure& e){
            t.send_message(Status(StatusCode::IOError, e.what()));
            leveldb::Log(logger, "socet error %s", e.what());
        }

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

    //  Launch pool of worker threads
    std::vector<std::unique_ptr<std::thread> > threads_vec;
    for (int thread_nbr = 0; thread_nbr < opt.get_threads_num(); thread_nbr++) {
        std::thread* tmp =new std::thread(worker_routine, &q_in, &q_out, &db, opt.get_logger());
        threads_vec.push_back(std::unique_ptr<std::thread>(tmp));
    }
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
    }
    for (auto it = threads_vec.begin() ; it != threads_vec.end() ; ++ it){
        (*it)->join();
    }
    return 0;
}


