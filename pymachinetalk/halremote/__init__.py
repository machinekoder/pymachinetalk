__import__('pkg_resources').declare_namespace(__name__)
from pymachinetalk.halremote.pin import Pin
from pymachinetalk.halremote.remotecomponent import RemoteComponent
from machinetalk.protobuf.types_pb2 import *


def component(name):
    return RemoteComponent(name)
