import zmq
import msgpack


class Command(object):
    END = 0
    GET = 1
    WRITE = 2
    RANGE = 3


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
        self.socket = self.context.socket(zmq.DEALER)
        # self.socket.setsockopt(zmq.IDENTITY, msgpack.packb('abc'))
        self.socket.connect("tcp://{0}:{1}".format(host, port))
        self.unpacker = msgpack.Unpacker()

    def get(self, key, verify_checksums=False, fill_cache=True):
        self.socket.send_multipart([msgpack.packb(Command.GET), msgpack.packb((verify_checksums, fill_cache)), msgpack.packb(key), msgpack.packb(Command.END)])
        res = []
        while True:
            buf = self.socket.recv()
            self.unpacker.feed(buf)
            for o in self.unpacker:
                if o == Command.END:
                    if len(res) == 1:
                        res.append(None)
                    return res
                res.append(o)

    def put(self, key, value, sync=False):
        self.socket.send_multipart([msgpack.packb(Command.WRITE), msgpack.packb((sync, )), msgpack.packb((False, key, value)), msgpack.packb(Command.END)])
        res = []
        while True:
            buf = self.socket.recv()
            self.unpacker.feed(buf)
            for o in self.unpacker:
                if o == Command.END:
                    return res
                res.append(o)

    def delete(self, key, sync=False):
        self.socket.send_multipart([msgpack.packb(Command.WRITE), msgpack.packb((sync, )), msgpack.packb((True, key, '')), msgpack.packb(Command.END)])
        res = []
        while True:
            buf = self.socket.recv()
            self.unpacker.feed(buf)
            for o in self.unpacker:
                if o == Command.END:
                    return res
                res.append(o)

    def write_batch(self, data, sync=False):
        self.socket.send_multipart([msgpack.packb(Command.WRITE), msgpack.packb((sync, ))], flags=zmq.SNDMORE)
        packer = msgpack.Packer()
        for key, value in data:
            self.socket.send(packer.pack((False, key, value)), flags=zmq.SNDMORE)
        self.socket.send(msgpack.packb(Command.END))
        res = []
        while True:
            buf = self.socket.recv()
            self.unpacker.feed(buf)
            for o in self.unpacker:
                if o == Command.END:
                    return res
                res.append(o)

    def delete_batch(self, keys, sync=False):
        self.socket.send_multipart([msgpack.packb(Command.WRITE), msgpack.packb((sync, ))], flags=zmq.SNDMORE)
        packer = msgpack.Packer()
        for key in keys:
            self.socket.send(packer.pack((True, key, '')), flags=zmq.SNDMORE)
        self.socket.send(msgpack.packb(Command.END))
        res = []
        while True:
            buf = self.socket.recv()
            self.unpacker.feed(buf)
            for o in self.unpacker:
                if o == Command.END:
                    return res
                res.append(o)

    def get_range(self, begin_key, end_key, verify_checksums=False, fill_cache=False):
        self.socket.send_multipart([msgpack.packb(Command.RANGE), msgpack.packb((verify_checksums, fill_cache)), msgpack.packb(begin_key),
                                    msgpack.packb(end_key), msgpack.packb(Command.END)])
        while True:
            buf = self.socket.recv()
            self.unpacker.feed(buf)
            for o in self.unpacker:
                if o == Command.END:
                    return
                yield o
