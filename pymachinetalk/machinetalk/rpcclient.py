import zmq
import threading
import uuid
from fysom import Fysom

import machinetalk.protobuf.types_pb2 as pb
from machinetalk.protobuf.message_pb2 import Container


class RpcClient(object):
    def __init__(self, debuglevel=0, debugname='RPC Client'):
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

        # Heartbeat
        self.heartbeat_lock = threading.Lock()
        self.heartbeat_period = 2500
        self.heartbeat_error_count = 0
        self.heartbeat_error_threshold = 2
        self.heartbeat_timer = None
        self.heartbeat_active = False

        # callbacks
        self.socket_message_received_cb = None
        self.state_changed_cb = None
        self.started = False

        # fsm
        self.fsm = Fysom({'initial': 'down',
                          'events': [
                            {'name': 'connect', 'src': 'down', 'dst': 'trying'},
                            {'name': 'connected', 'src': 'trying', 'dst': 'up'},
                            {'name': 'disconnect', 'src': 'trying', 'dst': 'down'},
                            {'name': 'timeout', 'src': 'up', 'dst': 'trying'},
                            {'name': 'refresh', 'src': 'up', 'dst': 'up'},
                            {'name': 'tick', 'src': 'up', 'dst': 'up'},
                            {'name': 'msg_sent', 'src': 'up', 'dst': 'up'},
                            {'name': 'disconnect', 'src': 'up', 'dst': 'down'},
                          ]})

        self.fsm.ondown = self.on_fsm_down
        self.fsm.onconnect = self.on_fsm_connect
        self.fsm.ontrying = self.on_fsm_trying
        self.fsm.onconnected = self.on_fsm_connected
        self.fsm.ondisconnect = self.on_fsm_disconnect
        self.fsm.onup = self.on_fsm_up
        self.fsm.ontimeout = self.on_fsm_timeout
        self.fsm.onrefresh = self.on_fsm_refresh
        self.fsm.ontick = self.on_fsm_tick
        self.fsm.onmsg_sent = self.on_fsm_msg_sent

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
        self.reset_heartbeat_error()
        self.send_ping()
        self.start_heartbeat_timer()
        return True

    def on_fsm_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state TRYING' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('trying')
        return True

    def on_fsm_connected(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event CONNECTED' % self.debugname)
        self.reset_heartbeat_error()
        self.reset_heartbeat_timer()
        return True

    def on_fsm_disconnect(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event DISCONNECT' % self.debugname)
        self.stop_heartbeat_timer()
        self.disconnect_sockets()
        return True

    def on_fsm_up(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state UP' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('up')
        return True

    def on_fsm_timeout(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event TIMEOUT' % self.debugname)
        self.disconnect_sockets()
        self.connect_sockets()
        self.send_ping()
        return True

    def on_fsm_refresh(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event REFRESH' % self.debugname)
        self.reset_heartbeat_error()
        return True

    def on_fsm_tick(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event TICK' % self.debugname)
        self.send_ping()
        return True

    def on_fsm_msg_sent(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event MSG SENT' % self.debugname)
        self.reset_heartbeat_timer()
        return True

    def socket_worker(self, context, uri):
        poll = zmq.Poller()
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(uri)
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

    def heartbeat_timer_tick(self):
        with self.heartbeat_lock:
            self.heartbeat_timer = None  # timer is dead on tick

        if self.debuglevel > 0:
            print('[%s] heartbeat timer tick' % self.debugname)

        self.heartbeat_error_count += 1
        if self.heartbeat_error_count > self.heartbeat_error_threshold:
            if self.fsm.isstate('up'):
                self.fsm.timeout()
            return
        if self.fsm.isstate('up'):
            self.fsm.tick()

    def reset_heartbeat_error(self):
        self.heartbeat_error_count = 0

    def reset_heartbeat_timer(self):
        if not self.heartbeat_active:
            return

        self.heartbeat_lock.acquire()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

        if self.heartbeat_period > 0:
            self.heartbeat_timer = threading.Timer(self.heartbeat_period / 1000.0,
                                                 self.heartbeat_timer_tick)
            self.heartbeat_timer.start()
        self.heartbeat_lock.release()
        if self.debuglevel > 0:
            print('[%s] heartbeat timer reset' % self.debugname)

    def start_heartbeat_timer(self):
        self.heartbeat_active = True
        self.reset_heartbeat_timer()

    def stop_heartbeat_timer(self):
        self.heartbeat_active = False
        self.heartbeat_lock.acquire()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None
        self.heartbeat_lock.release()

    # process all messages received on socket
    def socket_message_received(self, socket):
        msg = socket.recv()
        self.socket_rx.ParseFromString(msg)
        if self.debuglevel > 0:
            print('[%s] received message' % self.debugname)
            if self.debuglevel > 1:
                print(self.socket_rx)
        rx = self.socket_rx

        # react to any incoming message
        if self.fsm.isstate('trying'):
            self.fsm.connected()
        if self.fsm.isstate('up'):
            self.fsm.refresh()

        # react to ping acknowledge message
        if rx.type == pb.MT_PING_ACKNOWLEDGE:
            return   # ping acknowledge is uninteresting

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
        if self.fsm.isstate('up'):
            self.fsm.msg_sent()

    def send_ping(self):
        tx = self.socket_tx
        self.send_socket_message(pb.MT_PING, tx)

