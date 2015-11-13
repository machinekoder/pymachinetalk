import uuid
import platform

import zmq
import threading

# protobuf
from machinetalk.protobuf.message_pb2 import Container
from machinetalk.protobuf.types_pb2 import * 


class Key():
    def __init__(self):
        self.value = None
        self.name = ''
        self.keytype = None
        self.metadata = None
        self.synced = False
        self.parent = None
        self.synced_condition = threading.Condition(threading.Lock())
        self.value_condition = threading.Condition(threading.Lock())

        # callbacks
        self.on_synced_changed = []
        self.on_value_changed = []


class ParamClient():
    def __init__(self, basekey, debug=False):
        self.threads = []
        self.shutdown = threading.Event()
        self.tx_lock = threading.Lock()
        self.timer_lock = threading.Lock()
        self.connected_condition = threading.Condition(threading.Lock())

        self.debug = debug

        # callbacks
        self.on_connected_changed = []

        self.basekey = basekey
        self.keysbyname = {}
        self.is_ready = False

        self.param_uri = ''
        self.paramcmd_uri = ''
        self.connected = False
        self.heartbeat_period = 3000
        self.ping_outstanding = False
        self.state = 'Disconnected'
        self.param_state = 'Down'
        self.paramcmd_state = 'Down'
        self.param_period = 0
        self.param_timer = None
        self.paramcmd_timer = None

        # more efficient to reuse a protobuf message
        self.tx = Container()
        self.rx = Container()

        # ZeroMQ
        client_id = '%s-%s' % (platform.node(), uuid.uuid4())  # must be unique
        context = zmq.Context()
        context.linger = 0
        self.context = context
        self.paramcmd_socket = self.context.socket(zmq.DEALER)
        self.paramcmd_socket.setsockopt(zmq.LINGER, 0)
        self.paramcmd_socket.setsockopt(zmq.IDENTITY, client_id)
        self.param_socket = self.context.socket(zmq.SUB)
        self.sockets_connected = False

    def socket_worker(self):
        poll = zmq.Poller()
        poll.register(self.param_socket, zmq.POLLIN)
        poll.register(self.paramcmd_socket, zmq.POLLIN)

        while not self.shutdown.is_set():
            s = dict(poll.poll(200))
            if self.paramcmd_socket in s:
                self.process_paramcmd()
            if self.param_socket in s:
                self.process_param()

    def process_param(self):
        (topic, msg) = self.param_socket.recv_multipart()
        self.rx.ParseFromString(msg)

        if topic != self.basekey:  # ignore uninteresting messages
            return

        if self.debug:
            print('[%s] received message on halrcomp: topic %s' % (self.name, topic))
            print(self.rx)

        if self.rx.type == MT_PARAM_INCREMENTAL_UPDATE:
            pass

    def process_paramcmd(self):
        msg = self.paramcmd_socket.recv()
        self.rx.ParseFromString(msg)
        if self.debug:
            print('[param] received message on paramcmd:')
            print(self.rx)

        if self.rx.type == MT_PING_ACKNOWLEDGE:
            self.ping_outstanding = False
            if self.paramcmd_state == 'Trying':
                self.update_state('Connecting')
                #self.bind()

        else:
            print('[param] Warning: paramcmd receiced unsupported message')

    def start(self):
        self.paramcmd_state = 'Trying'
        self.update_state('Connecting')

        if self.connect_sockets():
            self.shutdown.clear()  # in case we already used the component
            self.threads.append(threading.Thread(target=self.socket_worker))
            for thread in self.threads:
                thread.start()
            self.start_paramcmd_heartbeat()
            with self.tx_lock:
                self.send_cmd(MT_PING)

    def stop(self):
        self.is_ready = False
        self.shutdown.set()
        for thread in self.threads:
            thread.join()
        self.threads = []
        self.cleanup()
        self.update_state('Disconnected')

    def cleanup(self):
        if self.connected:
            self.unsubscribe()
        self.stop_paramcmd_heartbeat()
        self.disconnect_sockets()

    def connect_sockets(self):
        self.sockets_connected = True
        self.paramcmd_socket.connect(self.paramcmd_uri)
        self.param_socket.connect(self.param_uri)

        return True

    def disconnect_sockets(self):
        if self.sockets_connected:
            self.paramcmd_socket.disconnect(self.paramcmd_uri)
            self.param_socket.disconnect(self.param_uri)
            self.sockets_connected = False

    def send_cmd(self, msg_type):
        self.tx.type = msg_type
        if self.debug:
            print('[%s] sending message: %s' % (self.basekey, msg_type))
            print(str(self.tx))
        self.paramcmd_socket.send(self.tx.SerializeToString(), zmq.NOBLOCK)
        self.tx.Clear()

    def paramcmd_timer_tick(self):
        if not self.connected:
            return

        if self.ping_outstanding:
            self.paramcmd_state = 'Trying'
            self.update_state('Timeout')

        with self.tx_lock:
            self.send_cmd(MT_PING)
        self.ping_outstanding = True

        self.paramcmd_timer = threading.Timer(self.heartbeat_period / 1000,
                                             self.paramcmd_timer_tick)
        self.paramcmd_timer.start()  # rearm timer

    def start_paramcmd_heartbeat(self):
        if not self.connected:
            return

        self.ping_outstanding = False

        if self.heartbeat_period > 0:
            self.paramcmd_timer = threading.Timer(self.heartbeat_period / 1000,
                                                  self.paramcmd_timer_tick)
            self.paramcmd_timer.start()

    def stop_paramcmd_heartbeat(self):
        if self.paramcmd_timer:
            self.paramcmd_timer.cancel()
            self.paramcmd_timer = None

    def param_timer_tick(self):
        self.param_state = 'Down'
        self.update_state('Timeout')

    def start_param_heartbeat(self, interval):
        self.timer_lock.acquire()
        if self.param_timer:
            self.param_timer.cancel()

        self.param_period = interval
        if interval > 0:
            self.param_timer = threading.Timer(interval / 1000,
                                               self.param_timer_tick)
            self.param_timer.start()
        self.timer_lock.release()

    def stop_param_heartbeat(self):
        self.timer_lock.acquire()
        if self.param_timer:
            self.param_timer.cancel()
            self.param_timer = None
        self.timer_lock.release()

    def refresh_param_heartbeat(self):
        self.timer_lock.acquire()
        if self.param_timer:
            self.param_timer.cancel()
            self.param_timer = threading.Timer(self.param_period / 1000,
                                               self.param_timer_tick)
            self.param_timer.start()
        self.timer_lock.release()

    def update_state(self, state):
        if state != self.state:
            self.state = state
            if state == 'Connected':
                with self.connected_condition:
                    self.connected = True
                    self.connected_condition.notify()
                print('[param] connected')
                for func in self.on_connected_changed:
                    func(self.connected)
            elif self.connected:
                with self.connected_condition:
                    self.connected = False
                    self.connected_condition.notify()
                self.stop_param_heartbeat()
                print('[param] disconnected')
                for func in self.on_connected_changed:
                    func(self.connected)
            elif state == 'Error':
                with self.connected_condition:
                    self.connected = False
                    self.connected_condition.notify()  # notify even if not connected

    def update_error(self, error, description):
        print('[param] error: %s %s' % (error, description))

    def ready(self):
        if not self.is_ready:
            self.is_ready = True
            self.start()

    def key_update(self, rpin, lpin):
        # if rpin.HasField('halfloat'):
        #     lpin.value = float(rpin.halfloat)
        #     lpin.synced = True
        # elif rpin.HasField('halbit'):
        #     lpin.value = bool(rpin.halbit)
        #     lpin.synced = True
        # elif rpin.HasField('hals32'):
        #     lpin.value = int(rpin.hals32)
        #     lpin.synced = True
        # elif rpin.HasField('halu32'):
        #     lpin.value = int(rpin.halu32)
        #     lpin.synced = True
        pass

    def key_change(self, key):
        if self.debug:
            print('[param] key change %s' % key.name)

        if self.state != 'Connected':  # accept only when connected
            return

        # This message MUST carry a Pin message for each pin which has
        # changed value since the last message of this type.
        # Each Pin message MUST carry the handle field.
        # Each Pin message MAY carry the name field.
        # Each Pin message MUST carry the type field
        # Each Pin message MUST - depending on pin type - carry a halbit,
        # halfloat, hals32, or halu32 field.
        with self.tx_lock:
            # TODO
            pass

    def subscribe(self):
        self.param_state = 'Trying'
        self.param_socket.setsockopt(zmq.SUBSCRIBE, self.basekey)

    def unsubscribe(self):
        self.param_state = 'Down'
        self.param_socket.setsockopt(zmq.UNSUBSCRIBE, self.basekey)

    def __getitem__(self, k):
        return self.keysbyname[k].get()

    def __setitem__(self, k, v):
        self.keysbyname[k].set(v)
