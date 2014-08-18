#ifndef ZMQWRAPPER_H
#define ZMQWRAPPER_H
#include <string>
#include <zmq.h>

class ZMQWrapper{
public:
    ZMQWrapper(int port, int fd);
    ~ZMQWrapper();

    void wait();
    bool recv_next(std::string* identity, std::string* message);
    bool send_next(std::string* identity, std::string* message);

private:
    void* context;
    void* socket;
    zmq_pollitem_t pollall[3];
};


#endif // ZMQWRAPPER_H
