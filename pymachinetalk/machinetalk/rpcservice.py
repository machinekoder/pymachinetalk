import zmq
import threading
import uuid
from fysom import Fysom

import machinetalk.protobuf.types_pb2 as pb
from machinetalk.protobuf.message_pb2 import Container


class RpcService(object):
    def __init__(self, debuglevel=0, debugname='RPC Service'):
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
        self.socket_rx = Container()
        self.socket_tx = Container()

        # callbacks
        self.socket_message_received_cb = None
        self.state_changed_cb = None
        self.started = False

        # fsm
        self.fsm = Fysom({'initial': 'down',
                          'events': [
                            {'name': 'connect', 'src': 'down', 'dst': 'up'},
                            {'name': 'ping_received', 'src': 'up', 'dst': 'up'},
                            {'name': 'disconnect', 'src': 'up', 'dst': 'down'},
                          ]})

        self.fsm.ondown = self.on_fsm_down
        self.fsm.onconnect = self.on_fsm_connect
        self.fsm.onup = self.on_fsm_up
        self.fsm.onping_received = self.on_fsm_ping_received
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

    def on_fsm_ping_received(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event PING RECEIVED' % self.debugname)
        self.send_ping_acknowledge()
        return True

    def on_fsm_disconnect(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event DISCONNECT' % self.debugname)
        self.disconnect_sockets()
        return True

    def socket_worker(self, context, uri):
        poll = zmq.Poller()
        socket = context.socket(zmq.ROUTER)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(uri)
        poll.register(socket, zmq.POLLIN)

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
                socket.send(pipe.recv(), zmq.NOBLOCK)
            if socket in s:
                self.socket_message_received(socket)

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

    # process all messages received on socket
    def socket_message_received(self, socket):
        msg = socket.recv()
        self.socket_rx.ParseFromString(msg)
        if self.debuglevel > 0:
            print('[%s] received message' % self.debugname)
            if self.debuglevel > 1:
                print(self.socket_rx)
        rx = self.socket_rx

        # react to ping message
        if rx.type == pb.MT_PING:
            if self.fsm.isstate('up'):
                self.fsm.ping_received()
            return   # ping is uninteresting

        if self.socket_message_received_cb:
            self.socket_message_received_cb(rx)

    def send_socket_message(self, msg_type, tx):
        with self.tx_lock:
            tx.type = msg_type
            if self.debuglevel > 0:
                print('[%s] sending message: %s' % (self.debugname, msg_type))
                if self.debuglevel > 1:
                    print(str(tx))

            self.pipe.send(tx.SerializeToString())
            tx.Clear()

    def send_ping_acknowledge(self):
        tx = self.socket_tx
        self.send_socket_message(pb.MT_PING_ACKNOWLEDGE, tx)

