import zmq
import msgpack


class Command(object):
    GET = 0
    WRITE = 1


class Status(object):
    codeOK = 0
    codeNotFound = 1
    codeCorruption = 2
    codeNotSupported = 3
    codeInvalidArgument = 4
    codeIOError = 5

    def __init__(self, packed_value):
        self.code = packed_value[0]
        self.reason = packed_value[1]

    def __repr__(self):
        if self.code == self.codeOK:
            return "OK"
        elif self.code == self.codeNotFound:
            return "NotFound"
        elif self.code == self.codeCorruption:
            return "Corruption ({0})".format(self.reason)
        elif self.code == self.codeNotSupported:
            return "NotSupported ({0})".format(self.reason)
        elif self.code == self.codeInvalidArgument:
            return "InvalidArgument ({0})".format(self.reason)
        elif self.code == self.codeIOError:
            return "IOError ({0})".format(self.reason)


class Turnip(object):
    def __init__(self, host='localhost', port=5544, io_threads=1):
        self.context = zmq.Context(io_threads=io_threads)
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://{0}:{1}".format(host, port))

    def get(self, key, verify_checksums=False, fill_cache=True):
        msg = msgpack.packb((Command.GET, [verify_checksums, fill_cache, key]))
        self.socket.send(msg)
        message = msgpack.unpackb(self.socket.recv())
        status = Status(message[0])
        if status.code != Status.codeOK:
            return status, None
        value = message[1]
        return status, value

    def put(self, key, value, sync=False):
        msg = msgpack.packb((Command.WRITE, [sync, [(False, key, value), ]]))
        self.socket.send(msg)
        message = msgpack.unpackb(self.socket.recv())
        status = Status(message[0])
        return status

    def delete(self, key, sync=False):
        msg = msgpack.packb((Command.WRITE, [sync, [(True, key, ''), ]]))
        self.socket.send(msg)
        message = msgpack.unpackb(self.socket.recv())
        status = Status(message[0])
        return status

    def write_batch(self, data, sync=False):
        msg = msgpack.packb((Command.WRITE, [sync, [(False, key, value) for key, value in data]]))
        self.socket.send(msg)
        message = msgpack.unpackb(self.socket.recv())
        status = Status(message[0])
        return status

    def delete_batch(self, keys, sync=False):
        msg = msgpack.packb((Command.WRITE, [sync, [(False, key, '') for key in keys]]))
        self.socket.send(msg)
        message = msgpack.unpackb(self.socket.recv())
        status = Status(message[0])
        return status
