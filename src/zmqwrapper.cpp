#include "zmqwrapper.h"
#include <sstream>


int HWM = 1000;
int MANDATORY = 1;


struct ZMQMessage{
    zmq_msg_t message;
    ZMQMessage(){
        zmq_msg_init (&message);
    }
    ~ZMQMessage(){
        zmq_msg_close (&message);
    }
};


ZMQWrapper::ZMQWrapper(int port, int fd){
    std::ostringstream oss;
    oss << "tcp://*:" << port;
    context = zmq_ctx_new ();
    socket = zmq_socket (context, ZMQ_ROUTER);
    zmq_setsockopt(socket, ZMQ_SNDHWM, &HWM, sizeof(int));
    zmq_setsockopt(socket, ZMQ_RCVHWM, &HWM, sizeof(int));
    zmq_setsockopt(socket, ZMQ_ROUTER_MANDATORY, &MANDATORY, sizeof(int));
    pollall[0].events = ZMQ_POLLIN;
    pollall[0].socket = socket;
    pollall[1].socket = NULL;
    pollall[1].fd = fd;
    pollall[1].events = ZMQ_POLLIN;
    pollall[2].events = ZMQ_POLLOUT;
    pollall[2].socket = socket;
    zmq_bind (socket, oss.str().c_str());
}
ZMQWrapper::~ZMQWrapper(){
    zmq_close (socket);
    zmq_ctx_destroy (context);
}

void ZMQWrapper::wait(){
    zmq_poll(pollall, 2, -1);
}

bool ZMQWrapper::recv_next(std::string* identity, std::string* message)
{
    int ret = zmq_poll(&pollall[0], 1, 0);
    if (ret < 1) {return false;}
    int more = 0;
    size_t more_size = sizeof (more);
    message->clear();
    do
    {
        ZMQMessage part;
        ret = zmq_msg_recv (&part.message, socket, ZMQ_DONTWAIT);
        if (ret == -1){
            if (zmq_errno() == EAGAIN){return false;}
            throw network_error("socket read error");
        }
        if (more == 0){identity->assign((char*)zmq_msg_data(&part.message), zmq_msg_size(&part.message));}
        else {message->append((char*)zmq_msg_data(&part.message), zmq_msg_size(&part.message));}
        ret = zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (ret == -1){ throw network_error("can't get socket options");}
    } while (more);
    return true;
}

bool ZMQWrapper::send_next(std::string* identity, std::string* message)
{
    int ret = zmq_poll (&pollall[2], 1, 0);
    if (ret < 1){
        return false;
    }
    ret = zmq_send (socket, identity->data(), identity->size(), ZMQ_SNDMORE | ZMQ_NOBLOCK);
    if (ret == (int)identity->size()) {ret += zmq_send (socket, NULL, 0, ZMQ_SNDMORE | ZMQ_NOBLOCK);}
    if (ret == (int)identity->size()) {ret += zmq_send (socket, message->data(), message->size(), ZMQ_NOBLOCK);}
    if (ret != (int)(identity->size() + message->size())){
        if (zmq_errno() == EAGAIN){return false;}
        else if (zmq_errno() == EHOSTUNREACH){return /*true*/ false;}
        throw network_error("can't send message");
    }
    return true;
}
