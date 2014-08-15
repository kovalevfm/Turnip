#ifndef QUEUE_H
#define QUEUE_H
#include <queue>
#include <string>
#include <unistd.h>
#include <queue>
#include <ios>
#include <stdexcept>

#include <thread>             // std::thread, std::this_thread::yield std::this_thread::sleep_for
#include <mutex>              // std::mutex, std::unique_lock
#include <condition_variable> // std::condition_variable
#include <chrono>         // std::chrono::seconds


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
        cv.notify_one();
    }

    int get_fd(){
        return pipefd[0];
    }

    void pop_front(T* t){
        std::lock_guard<std::mutex> lck (mtx);
        if (queue.size() > 0){
            t = queue.front();
            queue.pop();
            if (read(pipefd[0], &buf, 1) != 1) {throw std::ios_base::failure("bad read");}
            return true;
        }
        return false;
    }

    T block_pop(){
        while (true){
//            std::lock_guard<std::mutex> lck (mtx);
            std::unique_lock<std::mutex> lck_cv (mtx);
            while (queue.empty()) cv.wait(lck_cv);
            if (queue.size() > 0){
                T t = queue.front();
                queue.pop();
                if (read(pipefd[0], &buf, 1) != 1) {throw std::ios_base::failure("bad read");}
                return t;
            }
        }
    }


private:
    std::condition_variable cv;
//    std::mutex mtx_cv;
    std::queue<T> queue;
    int pipefd[2];
    std::mutex mtx;
    char buf;
};

#endif // QUEUE_H

