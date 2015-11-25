

class MessageObject():
    def __init__(self):
        self.is_position = False
        self.id_map = {}

    def __str__(self):
        output = ''
        for attr in dir(self)[3:]:
            output += '%s: %s\n' % (attr, getattr(self, attr))
        return output

    def __getitem__(self, index):
        if self.is_position:
            mapping = ['x', 'y', 'z', 'a', 'b', 'c', 'u', 'v', 'w']
            return getattr(self, mapping[index])
        else:
            raise RuntimeError("Object does not support indexed access")


def recurse_descriptor(descriptor, obj):
    for field in descriptor.fields:
        value = None

        if field.type == field.TYPE_BOOL:
            value = False
        elif field.type == field.TYPE_DOUBLE \
        or field.type == field.TYPE_FLOAT:
            value = 0.0
        elif field.type == field.TYPE_INT32 \
        or field.type == field.TYPE_INT64 \
        or field.type == field.TYPE_UINT32 \
        or field.type == field.TYPE_UINT64:
            value = 0
        elif field.type == field.TYPE_STRING:
            value = ''
        elif field.type == field.TYPE_ENUM:
            value = 0
        elif field.type == field.TYPE_MESSAGE:
            value = MessageObject()
            msg_descriptor = field.message_type
            if msg_descriptor.name == 'Position':
                value.is_position = True
            recurse_descriptor(msg_descriptor, value)

        if field.label == field.LABEL_REPEATED:
            delattr(value, 'index')
            attributes = dir(value)
            if len(attributes) == 4:  # only single attribute
                value = getattr(value, attributes[-1])
            value = [value]

        setattr(obj, field.name, value)
        obj.id_map[field.number] = field.name


def recurse_message(message, obj, field_filter=''):
    for descriptor in message.DESCRIPTOR.fields:
        filter_enabled = field_filter != ''
        # TODO: handle special file case here...

        if descriptor.number in obj.id_map:
            name = obj.id_map[descriptor.number]
        else:
            continue  # we do not know the object

        if filter_enabled and name != field_filter:
            continue

        if descriptor.label != descriptor.LABEL_REPEATED:
            if message.HasField(name):
                if descriptor.type == descriptor.TYPE_MESSAGE:
                    sub_obj = getattr(obj, name)
                    recurse_message(getattr(message, name), sub_obj)
                else:
                    setattr(obj, name, getattr(message, name))
        else:
            if descriptor.type == descriptor.TYPE_MESSAGE:
                array = getattr(obj, name)
                repeated = getattr(message, name)
                for sub_message in repeated:
                    index = sub_message.index

                    while len(array) < (index + 1):
                        array.append(MessageObject())

                    value = None
                    if len(sub_message.DESCRIPTOR.fields) == 2:
                        sub_obj = MessageObject()
                        recurse_descriptor(sub_message.DESCRIPTOR, sub_obj)
                        recurse_message(sub_message, sub_obj)
                        delattr(sub_obj, 'index')
                        value = getattr(sub_obj, dir(sub_obj)[-1])
                    else:
                        sub_obj = array[index]
                        recurse_message(sub_message, sub_obj)
                        value = sub_obj
                    array[index] = value


import threading
import zmq
import platform
import uuid
# protobuf
from machinetalk.protobuf.message_pb2 import Container
from machinetalk.protobuf.types_pb2 import *

DOWN = 0
TRYING = 1
UP = 2
TIMEOUT = 3


class Subscriber():
    def __init__(self, debuglevel=0, debugname=''):
        self.debuglevel = debuglevel
        self.debugname = debugname
        self.threads = []
        self.shutdown = threading.Event()
        #self.msg_lock = threading.Lock()
        self.timer_lock = threading.Lock()

        self.uri = ''
        self.service = ''
        self.heartbeat_period = 0
        self.heartbeat_timer = None
        self.state = DOWN
        self.topics = set()  # the topics we are interested in
        self.subscriptions = set()  # subscribed topics
        self.publishers = set()  # connected publishers

        # more efficient to reuse a protobuf message
        self.rx = Container()
        context = zmq.Context()
        context.linger = 0  # don't let messages linger after shutdown
        self.context = context
        self.socket = self.context.socket(zmq.SUB)

        self.started = False
        # callbacks
        self.message_received_cb = None
        self.state_changed_cb = None

    def socket_worker(self):
        poll = zmq.Poller()
        poll.register(self.socket, zmq.POLLIN)

        while not self.shutdown.is_set():
            s = dict(poll.poll(200))
            if self.socket in s:
                self.process_socket()

    def process_socket(self):
        (topic, msg) = self.socket.recv_multipart()
        self.rx.ParseFromString(msg)

        if self.debuglevel > 0:
            print('[%s] received message on %s: %i' % (self.debugname, topic, self.rx.type))
            if self.debuglevel > 1:
                print(str(msg))

        if self.rx.type == MT_PARAM_FULL_UPDATE:
            self.update_state(UP)

            if self.rx.HasField('pparams'):
                interval = self.rx.pparams.keepalive_timer
                self.heartbeat_period = interval * 2  # TODO: make configurable

        if self.state == UP:
            self.refresh_heartbeat()  # refresh heartbeat if any message is received
            if self.rx.type != MT_PING:
                self.message_received_cb(topic, self.rx)
        else:
            self.unsubscribe()  # clean up previous subscription
            self.subscribe()  # trigger a fresh subscribe -> full update

    def subscribe(self):
        self.update_state(TRYING)
        self.heartbeat_period = 0  # reset heartbeat
        for topic in self.topics:
            self.socket.setsockopt(zmq.SUBSCRIBE, topic)
            self.subscriptions.add(topic)

    def unsubscribe(self):
        self.update_state(DOWN)
        for topic in self.subscriptions:
            self.socket.setsockopt(zmq.UNSUBSCRIBE, topic)
        self.subscriptions.clear()

    def heartbeat_tick(self):
        self.update_state(TIMEOUT)

    def stop_heartbeat(self):
        self.timer_lock.acquire()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None
        self.timer_lock.release()

    def refresh_heartbeat(self):
        self.timer_lock.acquire()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

        if self.heartbeat_period > 0:
            self.heartbeat_timer = threading.Timer(self.heartbeat_period / 1000,
                                                   self.heartbeat_tick)
            self.heartbeat_timer.start()
        self.timer_lock.release()
        if self.debuglevel > 0:
            print('[%s] heartbeat updated' % self.debugname)

    def connect(self):
        if self.uri not in self.publishers:
            self.publishers.add(self.uri)
            for pub in self.publishers:  # in the future we may support multiple publishers
                self.socket.connect(pub)  # TODO: not sure if this can throw an error

        return True

    def disconnect(self):
        for pub in self.publishers:
            self.socket.disconnect(pub)
        self.publishers.clear()

    def update_state(self, state):
        if self.state == state:
            return
        self.state = state
        if self.state_changed_cb:
            self.state_changed_cb(state)
        if self.debuglevel > 0:
            states = {DOWN: 'DOWN', TRYING: 'TRYING', UP: 'UP', TIMEOUT: 'TIMEOUT'}
            print('[%s] updated state: %s' % (self.debugname, states[state]))

    def start(self):
        if self.started:
            return
        self.started = True
        if self.connect():
            self.shutdown.clear()  # in case we already used the component
            self.threads.append(threading.Thread(target=self.socket_worker))
            for thread in self.threads:
                thread.start()
            self.subscribe()

    def stop(self):
        if not self.started:
            return
        self.started = False
        self.update_state(DOWN)
        self.shutdown.set()
        for thread in self.threads:
            thread.join()  # join threads with the main thread
        self.threads = []
        self.stop_heartbeat()
        self.disconnect()


class Client():
    def __init__(self, debuglevel=0, debugname=''):
        self.debuglevel = debuglevel
        self.debugname = debugname
        self.threads = []
        self.shutdown = threading.Event()
        self.tx_lock = threading.Lock()
        self.timer_lock = threading.Lock()

        self.uri = ''
        self.service = ''
        self.heartbeat_period = 2500
        self.ping_error_count = 0
        self.ping_error_threshold = 2
        self.state = DOWN
        self.heartbeat_timer = None

        # more efficient to reuse a protobuf message
        self.rx = Container()
        self.tx = Container()

        # ZeroMQ
        client_id = '%s-%s' % (platform.node(), uuid.uuid4())  # must be unique
        context = zmq.Context()
        context.linger = 0
        self.context = context
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.IDENTITY, client_id)

        self.message_received_cb = None
        self.state_changed_cb = None
        self.started = False

    def socket_worker(self):
        poll = zmq.Poller()
        poll.register(self.socket, zmq.POLLIN)

        while not self.shutdown.is_set():
            s = dict(poll.poll(200))
            if self.socket in s:
                self.process_socket()

    def process_socket(self):
        msg = self.socket.recv()
        self.rx.ParseFromString(msg)
        if self.debuglevel > 0:
            print('[%s] received message from server' % self.debugname)
            if self.debuglevel > 1:
                print(self.rx)

        self.ping_error_count = 0  # any message counts as heartbeat since messages can be queued
        if self.state != UP:
            self.update_state(UP)
            # update state connecting

        if self.state == UP and self.rx.type != MT_PING_ACKNOWLEDGE:
            self.message_received_cb(self.rx)

    def start(self):
        if self.started:
            return
        self.started = True
        self.update_state(TRYING)
        if self.connect():
            self.shutdown.clear()
            self.threads.append(threading.Thread(target=self.socket_worker))
            for thread in self.threads:
                thread.start()
            self.ping_error_count = 0  # reset the error count
            self.refresh_heartbeat()  # start the heartbeat
            self.send_message(MT_PING, self.tx)

    def stop(self):
        if not self.started:
            return
        self.started = False
        self.update_state(DOWN)
        self.shutdown.set()
        for thread in self.threads:
            thread.join()
        self.threads = []
        self.stop_heartbeat()
        self.disconnect()

    def connect(self):
        self.service = self.uri  # make sure to save the uri we connected to
        self.socket.connect(self.service)
        return True

    def disconnect(self):
        self.socket.disconnect(self.service)

    def heartbeat_tick(self):
        self.send_message(MT_PING, self.tx)
        self.ping_error_count += 1

        if self.ping_error_count > self.ping_error_threshold and self.state == UP:
            self.update_state(TIMEOUT)

        self.timer_lock.acquire()
        self.heartbeat_timer = threading.Timer(self.heartbeat_period / 1000,
                                               self.heartbeat_tick)
        self.heartbeat_timer.start()  # rearm timer
        self.timer_lock.release()

    def refresh_heartbeat(self):
        self.timer_lock.acquire()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

        if self.heartbeat_period > 0:
            self.heartbeat_timer = threading.Timer(self.heartbeat_period / 1000,
                                                   self.heartbeat_tick)
            self.heartbeat_timer.start()
        self.timer_lock.release()
        if self.debuglevel > 0:
            print('[%s] heartbeat updated' % self.debugname)

    def stop_heartbeat(self):
        self.timer_lock.acquire()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None
        self.timer_lock.release()

    def update_state(self, state):
        if self.state == state:
            return
        self.state = state
        if self.state_changed_cb:
            self.state_changed_cb(state)
        if self.debuglevel > 0:
            states = {DOWN: 'DOWN', TRYING: 'TRYING', UP: 'UP', TIMEOUT: 'TIMEOUT'}
            print('[%s] updated state: %s' % (self.debugname, states[state]))

    def send_message(self, msg_type, tx):
        tx.type = msg_type
        if self.debuglevel > 0:
            print('[%s] sending message: %s' % (self.debugname, msg_type))
            if self.debuglevel > 1:
                print(str(self.tx))
        with self.tx_lock:
            self.socket.send(tx.SerializeToString(), zmq.NOBLOCK)
        tx.Clear()
        if msg_type != MT_PING:
            self.refresh_heartbeat()  # do not send pings when messages are sent
