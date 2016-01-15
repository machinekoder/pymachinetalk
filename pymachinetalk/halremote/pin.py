import threading
from machinetalk.protobuf.types_pb2 import *


class Pin():
    def __init__(self):
        self.name = ''
        self.pintype = HAL_BIT
        self.direction = HAL_IN
        self._synced = False
        self._value = None
        self.handle = 0  # stores handle received on bind
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
                self.parent.pin_change(self)

    def get(self):
        return self.value
