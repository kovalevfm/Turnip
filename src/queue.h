#ifndef QUEUE_H
#define QUEUE_H
#include <queue>
#include <string>
#include <mutex>
#include <condition_variable>
#include <unistd.h>
#include <sys/eventfd.h>
#include <poll.h>


template<typename T>
class Queue
{
public:
    Queue(size_t max_size_ = 1000) : max_size(max_size_){
    }

    bool empty() const{
        return queue.empty();
    }

    size_t size() const{
        return queue.size();
    }
    bool pop_nowait(T& t){
        std::lock_guard<std::mutex> lck (mtx);
        if (queue.size() > 0){
            t = queue.front();
            queue.pop();
            cv_pop.notify_one();
            return true;
        }
        return false;
    }
    T& front(){
        return queue.front();
    }

    const T& front() const{
        return queue.front();
    }
    bool push_nowait (const T& val){
        std::lock_guard<std::mutex> lck (mtx);
        if (queue.size() > max_size){
            return false;
        }
        queue.push(val);
        cv_push.notify_one();
        return true;
    }


    T block_pop(){
        while (true){
            std::unique_lock<std::mutex> lck_cv (mtx);
            while (queue.empty()) cv_push.wait(lck_cv);
            if (queue.size() > 0){
                T t = queue.front();
                queue.pop();
                cv_pop.notify_one();
                return t;
            }
        }
    }

    void block_push (const T& val){
        std::unique_lock<std::mutex> lck_cv (mtx);
        while (queue.size() > max_size) cv_pop.wait(lck_cv);
        queue.push(val);
        cv_push.notify_one();
    }

private:
    std::condition_variable cv_push;
    std::condition_variable cv_pop;
    std::queue<T> queue;
    std::mutex mtx;
    size_t max_size;
};


template<typename T>
class FDQueue
{
public:
    FDQueue(size_t max_size_ = 1000) : max_size(max_size_) {
        int fd = eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK);
        fds.fd = fd;
        fds.events = POLLIN;
    }
    ~FDQueue(){
        close(fds.fd);
    }

    bool empty() const{
        return queue.empty();
    }

    size_t size() const{
        return queue.size();
    }
    bool pop_nowait(T& t){
        uint64_t buf;
        if (read(fds.fd, &buf, sizeof(uint64_t)) == sizeof(uint64_t)){
            std::lock_guard<std::mutex> lck (mtx);
            t = queue.front();
            queue.pop();
            cv_pop.notify_one();
            return true;
        }
        return false;
    }

    T& front(){
        return queue.front();
    }

    const T& front() const{
        return queue.front();
    }
    bool push_nowait(const T& val){
        std::lock_guard<std::mutex> lck (mtx);
        if (queue.size() > max_size){
            return false;
        }
        queue.push(val);
        uint64_t buf = 1;
        if (write(fds.fd, &buf, sizeof(uint64_t)) <=0) {throw std::ios_base::failure("bad write");}
        return true;
    }

    int get_fd(){
        return fds.fd;
    }

    T block_pop(){
        uint64_t buf;
        while (true){
            int ret = poll(&fds, 1, -1);
            if (ret == POLLIN){
                std::unique_lock<std::mutex> lck (mtx);
                if (read(fds.fd, &buf, 8) > 0){
                    T t = queue.front();
                    queue.pop();
                    cv_pop.notify_one();
                    return t;
                }
            }
        }
    }

    void block_push (const T& val){
        std::unique_lock<std::mutex> lck_cv (mtx);
        while (queue.size() > max_size) cv_pop.wait(lck_cv);
        queue.push(val);
        uint64_t buf = 1;
        if (write(fds.fd, &buf, sizeof(uint64_t)) <=0) {throw std::ios_base::failure("bad write");}
    }

private:
    std::queue<T> queue;
    std::condition_variable cv_pop;
    struct pollfd fds;
    std::mutex mtx;
    size_t max_size;
};


#endif // QUEUE_H

