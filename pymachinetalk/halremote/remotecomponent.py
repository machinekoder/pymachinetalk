import threading
from pymachinetalk.halremote.remotecomponentbase import RemoteComponentBase
from pymachinetalk.halremote.pin import Pin

# protobuf
from machinetalk.protobuf.message_pb2 import Container
from machinetalk.protobuf.types_pb2 import *


class RemoteComponent(RemoteComponentBase):
    def __init__(self, name, debug=True):
        super(RemoteComponent, self).__init__()
        print(self.halrcmd_channel)
        self.tx_lock = threading.Lock()
        self.timer_lock = threading.Lock()
        self.connected_condition = threading.Condition(threading.Lock())
        self.debug = debug
        self.debuglevel = 1

        # callbacks
        self.on_connected_changed = []

        self.name = name
        self.pinsbyname = {}
        self.pinsbyhandle = {}
        self.is_ready = False
        self.no_create = False
        self.is_ready = False
        self.connected = False

        self.halrcmd_message_received_cb = self.halrcmd_socket_message_received
        self.halrcomp_message_received_cb = self.halrcomp_socket_message_received

        # more efficient to reuse a protobuf message
        self.tx = Container()
        #self.rx = Container()

    def wait_connected(self, timeout=None):
        with self.connected_condition:
            if self.connected:
                return True
            self.connected_condition.wait(timeout=timeout)
            return self.connected

    def halrcmd_socket_message_received(self, rx):
        if self.debug:
            print('[%s] received message on halrcmd:' % self.name)
            print(rx)

    def halrcomp_socket_message_received(self, topic, rx):
        if self.debug:
            print('[%s] received message on halrcomp: topic %s' % (self.name, topic))
            print(rx)

    def halrcomp_full_update_received(self, topic, rx):
        del topic
        comp = rx.comp[0]
        for rpin in comp.pin:
            name = rpin.name.split('.')[1]
            lpin = self.pinsbyname[name]
            lpin.handle = rpin.handle
            self.pinsbyhandle[rpin.handle] = lpin
            self.pin_update(rpin, lpin)

    def halrcomp_incremental_update_received(self, topic, rx):
        del topic
        for rpin in rx.pin:
            lpin = self.pinsbyhandle[rpin.handle]
            self.pin_update(rpin, lpin)

    def halrcommand_error_received(self, topic, rx):
        del topic
        print('error: ' + str(rx))

    # create a new HAL pin
    def newpin(self, name, pintype, direction):
        pin = Pin()
        pin.name = name
        pin.pintype = pintype
        pin.direction = direction
        pin.parent = self
        self.pinsbyname[name] = pin

        if pintype == HAL_FLOAT:
            pin.value = 0.0
        elif pintype == HAL_BIT:
            pin.value = False
        elif pintype == HAL_S32:
            pin.value = 0
        elif pintype == HAL_U32:
            pin.value = 0

        return pin

    def unsync_pins(self):
        for pin in self.pinsbyname:
            self.pinsbyname[pin].synced = False

    def getpin(self, name):
        return self.pinsbyname[name]

    def ready(self):
        if not self.is_ready:
            self.is_ready = True
            self.start()

    def add_pins(self):
        self.clear_halrcomp_topics()
        self.add_halrcomp_topic(self.name)

    def remove_pins(self):
        pass  # unused

    def synced(self):
        with self.connected_condition:
            self.connected = True
            self.connected_condition.notify()

    def pin_update(self, rpin, lpin):
        if rpin.HasField('halfloat'):
            lpin.value = float(rpin.halfloat)
            lpin.synced = True
        elif rpin.HasField('halbit'):
            lpin.value = bool(rpin.halbit)
            lpin.synced = True
        elif rpin.HasField('hals32'):
            lpin.value = int(rpin.hals32)
            lpin.synced = True
        elif rpin.HasField('halu32'):
            lpin.value = int(rpin.halu32)
            lpin.synced = True

    def pin_change(self, pin):
        if self.debug:
            print('[%s] pin change %s' % (self.name, pin.name))

        if not self.fsm.isstate('connected'):  # accept only when connected
            return
        if pin.direction == HAL_IN:  # only update out and IO pins
            return

        # This message MUST carry a Pin message for each pin which has
        # changed value since the last message of this type.
        # Each Pin message MUST carry the handle field.
        # Each Pin message MAY carry the name field.
        # Each Pin message MUST carry the type field
        # Each Pin message MUST - depending on pin type - carry a halbit,
        # halfloat, hals32, or halu32 field.
        with self.tx_lock:
            p = self.tx.pin.add()
            p.handle = pin.handle
            p.type = pin.pintype
            if p.type == HAL_FLOAT:
                p.halfloat = float(pin.value)
            elif p.type == HAL_BIT:
                p.halbit = bool(pin.value)
            elif p.type == HAL_S32:
                p.hals32 = int(pin.value)
            elif p.type == HAL_U32:
                p.halu32 = int(pin.value)
            self.send_halrcomp_set(self.tx)

    def bind(self):
        with self.tx_lock:
            c = self.tx.comp.add()
            c.name = self.name
            c.no_create = self.no_create  # for now we create the component
            for name, pin in self.pinsbyname.iteritems():
                p = c.pin.add()
                p.name = '%s.%s' % (self.name, name)
                p.type = pin.pintype
                p.dir = pin.direction
                if p.type == HAL_FLOAT:
                    p.halfloat = float(pin.value)
                elif p.type == HAL_BIT:
                    p.halbit = bool(pin.value)
                elif p.type == HAL_S32:
                    p.hals32 = int(pin.value)
                elif p.type == HAL_U32:
                    p.halu32 = int(pin.value)
            if self.debug:
                print('[%s] bind' % self.name)
            self.send_halrcomp_bind(self.tx)

    def __getitem__(self, k):
        return self.pinsbyname[k].get()

    def __setitem__(self, k, v):
        self.pinsbyname[k].set(v)
