import uuid
import platform

import zmq
import threading

# protobuf
from machinetalk.protobuf.message_pb2 import Container
from machinetalk.protobuf.types_pb2 import *
from machinetalk.protobuf.param_pb2 import *


class Key():
    def __init__(self):
        self.name = ''
        self.keytype = None
        self.metadata = None
        self._synced = False
        self._value = None
        self.parent = None
        self.synced_condition = threading.Condition(threading.Lock())
        self.value_condition = threading.Condition(threading.Lock())

        # callbacks
        self.on_synced_changed = []
        self.on_value_changed = []

    def wait_synced(self, timeout=None):
        with self.synced_condition:
            if self.synced:
                return True
            self.synced_condition.wait(timeout=timeout)
            return self.synced

    def wait_value(self, timeout=None):
        with self.value_condition:
            if self.value:
                return True
            self.value_condition.wait(timeout=timeout)
            return self.value

    @property
    def value(self):
        with self.value_condition:
            return self._value

    @value.setter
    def value(self, value):
        with self.value_condition:
            if self._value != value:
                self._value = value
                self.value_condition.notify()
                for func in self.on_value_changed:
                    func(value)

    @property
    def synced(self):
        with self.synced_condition:
            return self._synced

    @synced.setter
    def synced(self, value):
        with self.synced_condition:
            if value != self._synced:
                self._synced = value
                self.synced_condition.notify()
                for func in self.on_synced_changed:
                    func(value)

    def set(self, value):
        if self.value != value:
            self.value = value
            self.synced = False
            if self.parent:
                self.parent.key_change(self)

    def get(self):
        return self.value


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
            print('[%s] received message on param: topic %s' % (self.basekey, topic))
            print(self.rx)

        if self.rx.type == MT_PARAM_INCREMENTAL_UPDATE:
            for rkey in self.rx.key:
                if rkey.HasField('deleted') and rkey.deleted:
                    self.keysbyname.pop(rkey.name)
                else:
                    lkey = self.keysbyname[rkey.name]
                self.key_update(rkey, lkey)
            self.refresh_param_heartbeat()

        elif self.rx.type == MT_PARAM_FULL_UPDATE:
            for rkey in self.rx.key:
                name = rkey.name
                lkey = None
                if name in self.keysbyname:
                    lkey = self.keysbyname[name]
                else:
                    lkey = Key()
                    lkey.name = name
                    lkey.keytype = rkey.type
                    lkey.parent = self
                    self.keysbyname[name] = lkey
                #lkey.handle = rkey.hanlde
                #self.keysbyhandle[rkey.handle] = lkey
                self.key_update(rkey, lkey)

            if self.param_state != 'Up':  # will be executed only once
                self.param_state = 'Up'
                self.update_state('Connected')

            if self.rx.HasField('pparams'):
                interval = self.rx.pparams.keepalive_timer
                self.start_param_heartbeat(interval * 2)

        elif self.rx.type == MT_PARAM_ERROR:
            self.param_state = 'Down'
            self.update_state('Error')
            self.update_error('param', self.rx.note)

        elif self.rx.type == MT_PING:
            if self.param_state == 'Up':
                self.refresh_param_heartbeat()
            else:
                self.update_state('Connecting')
                self.unsubscribe()  # clean up previous subscription
                self.subscribe()  # trigger a fresh subscribe -> full update

        else:
            print('[%s] Warning: param receiced unsupported message' % self.basekey)

    def process_paramcmd(self):
        msg = self.paramcmd_socket.recv()
        self.rx.ParseFromString(msg)
        if self.debug:
            print('[%s] received message on paramcmd:' % self.basekey)
            print(self.rx)

        if self.rx.type == MT_PING_ACKNOWLEDGE:
            self.ping_outstanding = False
            if self.paramcmd_state == 'Trying':
                self.paramcmd_state = 'Up'
                self.update_state('Connecting')
                self.subscribe()

        else:
            print('[%s] Warning: paramcmd receiced unsupported message' % self.basekey)

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
        if not self.sockets_connected:
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
        if self.debug:
            print('[%s] timeout on param' % self.basekey)
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
                print('[%s] connected' % self.basekey)
                for func in self.on_connected_changed:
                    func(self.connected)
            elif self.connected:
                with self.connected_condition:
                    self.connected = False
                    self.connected_condition.notify()
                self.stop_param_heartbeat()
                self.unsync_keys()
                print('[%s] disconnected' % self.basekey)
                for func in self.on_connected_changed:
                    func(self.connected)
            elif state == 'Error':
                with self.connected_condition:
                    self.connected = False
                    self.connected_condition.notify()  # notify even if not connected

    def update_error(self, error, description):
        print('[%s] error: %s %s' % (self.basekey, error, description))

    def unsync_keys(self):
        for key in self.keysbyname:
            key.synced = False

    def ready(self):
        if not self.is_ready:
            self.is_ready = True
            self.start()

    def key_update(self, rkey, lkey):
        if rkey.HasField('paramstring'):
            lkey.value = str(rkey.paramstring)
            lkey.synced = True
        elif rkey.HasField('parambinary'):
            lkey.value = bytes(rkey.parambinary)
            lkey.synced = True
        elif rkey.HasField('paramint'):
            lkey.value = int(rkey.paramint)
            lkey.synced = True
        elif rkey.HasField('parambool'):
            lkey.value = bytes(rkey.parambool)
            lkey.synced = True
        elif rkey.HasField('paramdouble'):
            lkey.value = float(rkey.paramdouble)
            lkey.synced = True
        elif rkey.HasField('paramlist'):
            pass # TODO handle list

    def key_change(self, key):
        if self.debug:
            print('[%s] key change %s' % (self.basekey, key.name))

        if self.state != 'Connected':  # accept only when connected
            return

        # This message MUST carry a ParamKey message for each pin which has
        # changed value since the last message of this type.
        # Each Pin message MUST carry the name field.
        # Each Pin message MAY carry the name field.
        # Each Pin message MUST carry the type field
        # Each Pin message MUST - depending on pin type - carry a paramstring,
        # parambinary, paramint, parambool, paramdouble, paramlist field.
        with self.tx_lock:
            k = self.tx.key.add()
            k.name = key.name
            k.type = key.keytype
            if k.type == PARAM_STRING:
                k.paramstring = str(key.value)
            elif k.type == PARAM_BINARY:
                k.parambinary = bytes(key.value)
            elif k.type == PARAM_INTEGER:
                k.paramint = int(key.value)
            elif k.type == PARAM_DOUBLE:
                k.paramdoube = float(key.value)
            elif k.type == PARAM_LIST:
                pass  # TODO: implement
            self.send_cmd(MT_PARAM_SET)

    def getkey(self, name):
        return self.keysbyname[name]

    # create a new Key
    def newkey(self, name, keytype, value):
        if not self.connected:
            return

        key = Key()
        key.name = name
        key.keytype = keytype
        key.value = value
        key.parent = self
        self.keysbyname[name] = key
        self.key_change(key)

    # removes a key
    def rmkey(self, name):
        if not self.connected:
            return

        with self.tx_lock:
            self.tx.name = name
            self.send_cmd(MT_PARAM_DELETE)

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
