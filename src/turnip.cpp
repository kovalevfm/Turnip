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

#include <unistd.h>

#include <iostream>
#include <poll.h>


void worker_routine(InQueue* q_in,  OutQueue* q_out, LDB* db, leveldb::Logger* logger){
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
    InQueue q_in(1000);
    OutQueue q_out(1000);
    ZMQWrapper zmq_server(opt.get_port(), q_out.get_fd());
    std::string buff_identity;
    std::string buff_message;

    //  Launch pool of worker threads
    std::vector<std::unique_ptr<std::thread> > threads_vec;
    for (int thread_nbr = 0; thread_nbr < opt.get_threads_num(); thread_nbr++) {
        std::thread* tmp =new std::thread(worker_routine, &q_in, &q_out, &db, opt.get_logger());
        threads_vec.push_back(std::unique_ptr<std::thread>(tmp));
    }
    leveldb::Log(opt.get_logger(), "start server...");
    IdMesage buf;
    while (true){
        zmq_server.wait();
        if (zmq_server.recv_next(&buff_identity, &buff_message)){
            q_in.block_push(std::make_pair(buff_identity, buff_message));
        }
        if (!q_out.empty()){
            if (q_out.pop_nowait(buf)){
                while (!zmq_server.send_next(&buf.first, &buf.second)){}
            }
        }
    }
    for (auto it = threads_vec.begin() ; it != threads_vec.end() ; ++ it){
        (*it)->join();
    }

    return 0;
}


