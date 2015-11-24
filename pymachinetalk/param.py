import threading

# protobuf
from pymachinetalk.common import Subscriber
from pymachinetalk.common import Client
import pymachinetalk.common as common
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
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    TIMEOUT = 3
    ERROR = 4

    def __init__(self, basekey, debug=False):
        self.threads = []
        self.connected_condition = threading.Condition(threading.Lock())

        self.debug = debug

        # callbacks
        self.on_connected_changed = []

        self.basekey = basekey
        self.keysbyname = {}
        self.is_ready = False
        self.state = self.DISCONNECTED

        debuglevel = (debug) * 1
        self.subscriber = Subscriber(debuglevel=debuglevel, debugname='param')
        self.subscriber.topics.add(basekey)
        self.subscriber.message_received_cb = self.param_msg_received
        self.subscriber.state_changed_cb = self.socket_state_changed
        self.client = Client(debuglevel=debuglevel, debugname='paramcmd')
        self.client.message_received_cb = self.paramcmd_msg_received
        self.client.state_changed_cb = self.socket_state_changed

        self.connected = False
        self.param_uri = ''
        self.paramcmd_uri = ''

        # more efficient to reuse a protobuf message
        self.tx = Container()

    def param_msg_received(self, topic, rx):
        if self.debug:
            print('[%s] received message on param: topic %s' % (self.basekey, topic))
            #print(self.rx)

        if rx.type == MT_PARAM_INCREMENTAL_UPDATE:
            for rkey in rx.key:
                if rkey.HasField('deleted') and rkey.deleted:
                    self.keysbyname.pop(rkey.name)
                else:
                    lkey = self.keysbyname[rkey.name]
                    self.key_update(rkey, lkey)

        elif rx.type == MT_PARAM_FULL_UPDATE:
            for rkey in rx.key:
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

        elif rx.type == MT_PARAM_ERROR:
            self.update_state(self.ERROR)
            self.update_error('param', rx.note)

        else:
            print('[%s] Warning: param receiced unsupported message' % self.basekey)

    def paramcmd_msg_received(self, rx):
        if self.debug:
            print('[%s] received message on paramcmd:' % self.basekey)
            #print(self.rx)

        if rx.type == MT_PING_ACKNOWLEDGE:  # we never will receive this here
            pass

        else:
            print('[%s] Warning: paramcmd receiced unsupported message' % self.basekey)

    def start(self):
        self.update_state(self.CONNECTING)
        self.subscriber.uri = self.param_uri
        self.subscriber.start()
        self.client.uri = self.paramcmd_uri
        self.client.start()

    def stop(self):
        self.is_ready = False
        self.subscriber.stop()
        self.client.stop()
        self.update_state(self.DISCONNECTED)

    def socket_state_changed(self, state):
        del state
        sub_state = self.subscriber.state
        cli_state = self.client.state
        if sub_state == common.UP and cli_state == common.UP:
            self.update_state(self.CONNECTED)
        else:
            self.update_state(self.CONNECTING)

    def update_state(self, state):
        if state != self.state:
            self.state = state
            if state == self.CONNECTED:
                with self.connected_condition:
                    self.connected = True
                    self.connected_condition.notify()
                if self.debug:
                    print('[%s] connected' % self.basekey)
                for func in self.on_connected_changed:
                    func(self.connected)
            elif self.connected:
                with self.connected_condition:
                    self.connected = False
                    self.connected_condition.notify()
                self.unsync_keys()
                if self.debug:
                    print('[%s] disconnected' % self.basekey)
                for func in self.on_connected_changed:
                    func(self.connected)
            elif state == self.ERROR:
                with self.connected_condition:
                    self.connected = False
                    self.connected_condition.notify()  # notify even if not connected

    def update_error(self, error, description):
        print('[%s] error: %s %s' % (self.basekey, error, description))

    def unsync_keys(self):
        for key in self.keysbyname:
            self.keysbyname[key].synced = False

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

        if self.state != self.CONNECTED:  # accept only when connected
            return

        # This message MUST carry a ParamKey message for each pin which has
        # changed value since the last message of this type.
        # Each Pin message MUST carry the name field.
        # Each Pin message MAY carry the name field.
        # Each Pin message MUST carry the type field
        # Each Pin message MUST - depending on pin type - carry a paramstring,
        # parambinary, paramint, parambool, paramdouble, paramlist field.
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
        self.client.send_message(MT_PARAM_SET, self.tx)

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
        self.key_change(key)  # the key change sends a message

    # removes a key
    def rmkey(self, name):
        if not self.connected:
            return

        self.tx.name = name
        self.client.send_message(MT_PARAM_DELETE, self.tx)

    def __getitem__(self, k):
        return self.keysbyname[k].get()

    def __setitem__(self, k, v):
        self.keysbyname[k].set(v)
