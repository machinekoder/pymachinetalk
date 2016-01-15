import zmq
import threading
import uuid
from fysom import Fysom

import machinetalk.protobuf.types_pb2 as pb
from machinetalk.protobuf.message_pb2 import Container


class Publish(object):
    def __init__(self, debuglevel=0, debugname='Publish'):
        self.debuglevel = debuglevel
        self.debugname = debugname
        self.error_string = ''
        # ZeroMQ
        context = zmq.Context()
        context.linger = 0
        self.context = context
        # pipe to signalize a shutdown
        self.shutdown = context.socket(zmq.PUSH)
        self.shutdown_uri = b'inproc://shutdown-%s' % uuid.uuid4()
        self.shutdown.bind(self.shutdown_uri)
        # pipe for outgoing messages
        self.pipe = context.socket(zmq.PUSH)
        self.pipe_uri = b'inproc://pipe-%s' % uuid.uuid4()
        self.pipe.bind(self.pipe_uri)
        self.thread = None  # socket worker tread
        self.tx_lock = threading.Lock()  # lock for outgoing messages

        # Socket
        self.socket_uri = ''
        # more efficient to reuse protobuf messages
        self.socket_tx = Container()

        # callbacks
        self.state_changed_cb = None
        self.started = False

        # fsm
        self.fsm = Fysom({'initial': 'down',
                          'events': [
                            {'name': 'connect', 'src': 'down', 'dst': 'up'},
                            {'name': 'disconnect', 'src': 'up', 'dst': 'down'},
                          ]})

        self.fsm.ondown = self.on_fsm_down
        self.fsm.onconnect = self.on_fsm_connect
        self.fsm.onup = self.on_fsm_up
        self.fsm.ondisconnect = self.on_fsm_disconnect

    def on_fsm_down(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state DOWN' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('down')
        return True

    def on_fsm_connect(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event CONNECT' % self.debugname)
        self.connect_sockets()
        return True

    def on_fsm_up(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state UP' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('up')
        return True

    def on_fsm_disconnect(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event DISCONNECT' % self.debugname)
        self.disconnect_sockets()
        return True

    def socket_worker(self, context, uri):
        poll = zmq.Poller()
        socket = context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(uri)

        shutdown = context.socket(zmq.PULL)
        shutdown.connect(self.shutdown_uri)
        poll.register(shutdown, zmq.POLLIN)
        pipe = context.socket(zmq.PULL)
        pipe.connect(self.pipe_uri)
        poll.register(pipe, zmq.POLLIN)

        while True:
            s = dict(poll.poll())
            if shutdown in s:
                shutdown.recv()
                return  # shutdown signal
            if pipe in s:
                socket.send_multipart(pipe.recv_multipart(), zmq.NOBLOCK)

    def connect_sockets(self):
        self.thread = threading.Thread(target=self.socket_worker,
                                       args=(self.context, self.socket_uri,))
        self.thread.start()

    def disconnect_sockets(self):
        self.shutdown.send('')  # trigger socket thread shutdown
        self.thread.join()
        self.thread = None

    def start(self):
        if self.started:
            return
        self.fsm.connect()  # todo
        self.started = True

    def stop(self):
        if not self.started:
            return
        self.fsm.disconnect()
        self.started = False

    def send_socket_message(self, topic, msg_type, tx):
        with self.tx_lock:
            tx.type = msg_type
            if self.debuglevel > 0:
                print('[%s] sending message: %s' % (self.debugname, msg_type))
                if self.debuglevel > 1:
                    print(str(tx))

            self.pipe.send_multipart([topic, tx.SerializeToString()])
            tx.Clear()

    def send_ping(self, topic):
        tx = self.socket_tx
        self.send_socket_message(topic, pb.MT_PING, tx)

    def send_full_update(self, topic, tx):
        self.send_socket_message(topic, pb.MT_FULL_UPDATE, tx)

    def send_incremental_update(self, topic, tx):
        self.send_socket_message(topic, pb.MT_INCREMENTAL_UPDATE, tx)

