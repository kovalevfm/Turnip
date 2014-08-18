#ifndef QUEUE_H
#define QUEUE_H
#include <queue>
#include <string>
#include <unistd.h>
#include <queue>
#include <ios>
#include <stdexcept>
#include <thread>
#include <mutex>
#include <condition_variable>


#include <iostream>

template<typename T>
class Queue
{
public:
    Queue(){
        if (pipe(pipefd) == -1){throw std::ios_base::failure("can't open pipe");}
    }

    bool empty() const{
        return queue.empty();
    }

    size_t size() const{
        return queue.size();
    }
    void pop(){
        std::lock_guard<std::mutex> lck (mtx);
        if (queue.size() > 0){
            queue.pop();
            if (read(pipefd[0], &buf, 1) != 1) {throw std::ios_base::failure("bad read");}
            cv_pop.notify_one();
        }
    }
    T& front(){
        return queue.front();
    }

    const T& front() const{
        return queue.front();
    }
    void push (const T& val){
        std::lock_guard<std::mutex> lck (mtx);
        queue.push(val);
        if (write(pipefd[1], &buf, 1) != 1) {throw std::ios_base::failure("bad write");}
        cv_push.notify_one();
    }

    int get_fd(){
        return pipefd[0];
    }

    T block_pop(){
        while (true){
            std::unique_lock<std::mutex> lck_cv (mtx);
            while (queue.empty()) cv_push.wait(lck_cv);
            if (queue.size() > 0){
                T t = queue.front();
                queue.pop();
                if (read(pipefd[0], &buf, 1) != 1) {throw std::ios_base::failure("bad read");}
                cv_pop.notify_one();
                return t;
            }
        }
    }

    void block_push (const T& val){
        std::unique_lock<std::mutex> lck_cv (mtx);
        while (queue.size() > 60000) cv_pop.wait(lck_cv);
        queue.push(val);
        if (write(pipefd[1], &buf, 1) != 1) {throw std::ios_base::failure("bad write");}
        cv_push.notify_one();
    }

private:
    std::condition_variable cv_push;
    std::condition_variable cv_pop;
    std::queue<T> queue;
    int pipefd[2];
    std::mutex mtx;
    char buf;
};

#endif // QUEUE_H

