import zmq
import threading
import uuid
from fysom import Fysom
from machinetalk.rpcclient import RpcClient
from machinetalk.subscribe import Subscribe
from machinetalk.publish import Publish

import machinetalk.protobuf.types_pb2 as pb
from machinetalk.protobuf.message_pb2 import Container


class SyncClient(object):
    def __init__(self, debuglevel=0, debugname='Sync Client'):
        self.debuglevel = debuglevel
        self.debugname = debugname
        self.error_string = ''

        # Sync
        self.sync_channel = RpcClient()
        self.sync_channel.debugname = '%s - %s' % (self.debugname, 'sync')
        self.sync_channel.state_changed_cb = self.sync_channel_state_changed
        self.sync_channel.socket_message_received_cb = self.sync_channel_message_received
        # more efficient to reuse protobuf messages
        self.sync_rx = Container()
        self.sync_tx = Container()

        # Sub
        self.sub_channel = Subscribe()
        self.sub_channel.debugname = '%s - %s' % (self.debugname, 'sub')
        self.sub_channel.state_changed_cb = self.sub_channel_state_changed
        self.sub_channel.socket_message_received_cb = self.sub_channel_message_received
        # more efficient to reuse protobuf messages
        self.sub_rx = Container()

        # Pub
        self.pub_channel = Publish()
        self.pub_channel.debugname = '%s - %s' % (self.debugname, 'pub')
        # more efficient to reuse protobuf messages
        self.pub_tx = Container()

        # callbacks
        self.sync_message_received_cb = None
        self.sub_message_received_cb = None
        self.state_changed_cb = None
        self.started = False

        # fsm
        self.fsm = Fysom({'initial': 'down',
                          'events': [
                            {'name': 'connect', 'src': 'down', 'dst': 'trying'},
                            {'name': 'sync_up', 'src': 'trying', 'dst': 'syncing'},
                            {'name': 'disconnect', 'src': 'trying', 'dst': 'down'},
                            {'name': 'sync_trying', 'src': 'syncing', 'dst': 'trying'},
                            {'name': 'sub_up', 'src': 'syncing', 'dst': 'synced'},
                            {'name': 'disconnect', 'src': 'syncing', 'dst': 'down'},
                            {'name': 'sub_trying', 'src': 'synced', 'dst': 'syncing'},
                            {'name': 'sync_trying', 'src': 'synced', 'dst': 'trying'},
                            {'name': 'disconnect', 'src': 'synced', 'dst': 'down'},
                          ]})

        self.fsm.ondown = self.on_fsm_down
        self.fsm.onconnect = self.on_fsm_connect
        self.fsm.ontrying = self.on_fsm_trying
        self.fsm.onsync_up = self.on_fsm_sync_up
        self.fsm.ondisconnect = self.on_fsm_disconnect
        self.fsm.onsyncing = self.on_fsm_syncing
        self.fsm.onsync_trying = self.on_fsm_sync_trying
        self.fsm.onsub_up = self.on_fsm_sub_up
        self.fsm.onsynced = self.on_fsm_synced
        self.fsm.onsub_trying = self.on_fsm_sub_trying

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
        self.start_sync_channel()
        self.start_pub_channel()
        return True

    def on_fsm_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state TRYING' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('trying')
        return True

    def on_fsm_sync_up(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event SYNC UP' % self.debugname)
        self.send_sync()
        self.start_sub_channel()
        return True

    def on_fsm_disconnect(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event DISCONNECT' % self.debugname)
        self.stop_sync_channel()
        self.stop_sub_channel()
        self.stop_pub_channel()
        return True

    def on_fsm_syncing(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state SYNCING' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('syncing')
        return True

    def on_fsm_sync_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event SYNC TRYING' % self.debugname)
        self.stop_sub_channel()
        return True

    def on_fsm_sub_up(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event SUB UP' % self.debugname)
        self.synced()
        return True

    def on_fsm_synced(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state SYNCED' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('synced')
        return True

    def on_fsm_sub_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event SUB TRYING' % self.debugname)
        self.send_sync()
        return True

    @property
    def sync_uri(self):
        return self.sync_channel.socket_uri

    @sync_uri.setter
    def sync_uri(self, value):
        self.sync_channel.socket_uri = value

    @property
    def sub_uri(self):
        return self.sub_channel.socket_uri

    @sub_uri.setter
    def sub_uri(self, value):
        self.sub_channel.socket_uri = value

    @property
    def pub_uri(self):
        return self.pub_channel.socket_uri

    @pub_uri.setter
    def pub_uri(self, value):
        self.pub_channel.socket_uri = value

    def add_sub_topic(self, name):
        self.sub_channel.add_socket_topic(name)

    def remove_sub_topic(self, name):
        self.sub_channel.remove_socket_topic(name)

    def clear_sub_topics(self):
        self.sub_channel.clear_socket_topics()

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

    def start_sync_channel(self):
        self.sync_channel.start()

    def stop_sync_channel(self):
        self.sync_channel.stop()

    def start_sub_channel(self):
        self.sub_channel.start()

    def stop_sub_channel(self):
        self.sub_channel.stop()

    def start_pub_channel(self):
        self.pub_channel.start()

    def stop_pub_channel(self):
        self.pub_channel.stop()

    # process all messages received on sync
    def sync_channel_message_received(self, rx):

        if self.sync_message_received_cb:
            self.sync_message_received_cb(rx)

    # process all messages received on sub
    def sub_channel_message_received(self, topic, rx):

        if self.sub_message_received_cb:
            self.sub_message_received_cb(topic, rx)

    def send_sync_message(self, msg_type, tx):
        self.sync_channel.send_socket_message(msg_type, tx)

    def send_pub_message(self, topic, msg_type, tx):
        self.pub_channel.send_socket_message(topic, msg_type, tx)

    def send_sync(self):
        tx = self.sync_tx
        self.send_sync_message(pb.MT_SYNC, tx)

    def send_incremental_update(self, topic, tx):
        self.send_pub_message(topic, pb.MT_INCREMENTAL_UPDATE, tx)

    def sync_channel_state_changed(self, state):

        if (state == 'trying'):
            if self.fsm.isstate('syncing'):
                self.fsm.sync_trying()
            if self.fsm.isstate('synced'):
                self.fsm.sync_trying()

        if (state == 'up'):
            if self.fsm.isstate('trying'):
                self.fsm.sync_up()

    def sub_channel_state_changed(self, state):

        if (state == 'trying'):
            if self.fsm.isstate('synced'):
                self.fsm.sub_trying()

        if (state == 'up'):
            if self.fsm.isstate('syncing'):
                self.fsm.sub_up()

