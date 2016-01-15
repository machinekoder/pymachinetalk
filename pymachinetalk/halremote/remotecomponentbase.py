import zmq
import threading
import uuid
from fysom import Fysom
from pymachinetalk.machinetalk.rpcclient import RpcClient
from pymachinetalk.halremote.halrcompsubscribe import HalrcompSubscribe

import machinetalk.protobuf.types_pb2 as pb
from machinetalk.protobuf.message_pb2 import Container


class RemoteComponentBase(object):
    def __init__(self, debuglevel=0, debugname='Remote Component Base'):
        self.debuglevel = debuglevel
        self.debugname = debugname
        self.error_string = ''

        # Halrcmd
        self.halrcmd_channel = RpcClient()
        self.halrcmd_channel.debugname = '%s - %s' % (self.debugname, 'halrcmd')
        self.halrcmd_channel.state_changed_cb = self.halrcmd_channel_state_changed
        self.halrcmd_channel.socket_message_received_cb = self.halrcmd_channel_message_received
        # more efficient to reuse protobuf messages
        self.halrcmd_rx = Container()
        self.halrcmd_tx = Container()

        # Halrcomp
        self.halrcomp_channel = HalrcompSubscribe()
        self.halrcomp_channel.debugname = '%s - %s' % (self.debugname, 'halrcomp')
        self.halrcomp_channel.state_changed_cb = self.halrcomp_channel_state_changed
        self.halrcomp_channel.socket_message_received_cb = self.halrcomp_channel_message_received
        # more efficient to reuse protobuf messages
        self.halrcomp_rx = Container()

        # callbacks
        self.halrcmd_message_received_cb = None
        self.halrcomp_message_received_cb = None
        self.state_changed_cb = None
        self.started = False

        # fsm
        self.fsm = Fysom({'initial': 'down',
                          'events': [
                            {'name': 'connect', 'src': 'down', 'dst': 'trying'},
                            {'name': 'halrcmd_up', 'src': 'trying', 'dst': 'binding'},
                            {'name': 'disconnect', 'src': 'trying', 'dst': 'down'},
                            {'name': 'bind_confirmed', 'src': 'binding', 'dst': 'syncing'},
                            {'name': 'bind_rejected', 'src': 'binding', 'dst': 'error'},
                            {'name': 'no_bind', 'src': 'binding', 'dst': 'syncing'},
                            {'name': 'halrcmd_trying', 'src': 'binding', 'dst': 'trying'},
                            {'name': 'disconnect', 'src': 'binding', 'dst': 'down'},
                            {'name': 'halrcmd_trying', 'src': 'syncing', 'dst': 'trying'},
                            {'name': 'halrcomp_up', 'src': 'syncing', 'dst': 'synced'},
                            {'name': 'sync_failed', 'src': 'syncing', 'dst': 'error'},
                            {'name': 'disconnect', 'src': 'syncing', 'dst': 'down'},
                            {'name': 'halrcomp_trying', 'src': 'synced', 'dst': 'syncing'},
                            {'name': 'halrcmd_trying', 'src': 'synced', 'dst': 'trying'},
                            {'name': 'set_rejected', 'src': 'synced', 'dst': 'error'},
                            {'name': 'disconnect', 'src': 'synced', 'dst': 'down'},
                            {'name': 'disconnect', 'src': 'error', 'dst': 'down'},
                          ]})

        self.fsm.ondown = self.on_fsm_down
        self.fsm.onconnect = self.on_fsm_connect
        self.fsm.ontrying = self.on_fsm_trying
        self.fsm.onhalrcmd_up = self.on_fsm_halrcmd_up
        self.fsm.ondisconnect = self.on_fsm_disconnect
        self.fsm.onbinding = self.on_fsm_binding
        self.fsm.onbind_confirmed = self.on_fsm_bind_confirmed
        self.fsm.onbind_rejected = self.on_fsm_bind_rejected
        self.fsm.onno_bind = self.on_fsm_no_bind
        self.fsm.onhalrcmd_trying = self.on_fsm_halrcmd_trying
        self.fsm.onsyncing = self.on_fsm_syncing
        self.fsm.onhalrcomp_up = self.on_fsm_halrcomp_up
        self.fsm.onsync_failed = self.on_fsm_sync_failed
        self.fsm.onsynced = self.on_fsm_synced
        self.fsm.onhalrcomp_trying = self.on_fsm_halrcomp_trying
        self.fsm.onset_rejected = self.on_fsm_set_rejected
        self.fsm.onerror = self.on_fsm_error

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
        self.add_pins()
        self.start_halrcmd_channel()
        return True

    def on_fsm_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state TRYING' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('trying')
        return True

    def on_fsm_halrcmd_up(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event HALRCMD UP' % self.debugname)
        self.bind()
        return True

    def on_fsm_disconnect(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event DISCONNECT' % self.debugname)
        self.stop_halrcmd_channel()
        self.stop_halrcomp_channel()
        self.remove_pins()
        return True

    def on_fsm_binding(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state BINDING' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('binding')
        return True

    def on_fsm_bind_confirmed(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event BIND CONFIRMED' % self.debugname)
        self.start_halrcomp_channel()
        return True

    def on_fsm_bind_rejected(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event BIND REJECTED' % self.debugname)
        self.stop_halrcmd_channel()
        return True

    def on_fsm_no_bind(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event NO BIND' % self.debugname)
        self.start_halrcomp_channel()
        return True

    def on_fsm_halrcmd_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event HALRCMD TRYING' % self.debugname)
        return True

    def on_fsm_syncing(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state SYNCING' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('syncing')
        return True

    def on_fsm_halrcomp_up(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event HALRCOMP UP' % self.debugname)
        self.synced()
        return True

    def on_fsm_sync_failed(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event SYNC FAILED' % self.debugname)
        self.stop_halrcomp_channel()
        self.stop_halrcmd_channel()
        return True

    def on_fsm_synced(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state SYNCED' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('synced')
        return True

    def on_fsm_halrcomp_trying(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event HALRCOMP TRYING' % self.debugname)
        self.unsync_pins()
        return True

    def on_fsm_set_rejected(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: event SET REJECTED' % self.debugname)
        self.stop_halrcomp_channel()
        self.stop_halrcmd_channel()
        return True

    def on_fsm_error(self, e):
        del e
        if self.debuglevel > 0:
            print('[%s]: state ERROR' % self.debugname)
        if self.state_changed_cb:
            self.state_changed_cb('error')
        return True

    @property
    def halrcmd_uri(self):
        return self.halrcmd_channel.socket_uri

    @halrcmd_uri.setter
    def halrcmd_uri(self, value):
        self.halrcmd_channel.socket_uri = value

    @property
    def halrcomp_uri(self):
        return self.halrcomp_channel.socket_uri

    @halrcomp_uri.setter
    def halrcomp_uri(self, value):
        self.halrcomp_channel.socket_uri = value

    def bind(self):
        print('WARNING: slot bind unimplemented')

    def add_pins(self):
        print('WARNING: slot add pins unimplemented')

    def remove_pins(self):
        print('WARNING: slot remove pins unimplemented')

    def unsync_pins(self):
        print('WARNING: slot unsync pins unimplemented')

    def synced(self):
        print('WARNING: slot synced unimplemented')

    def no_bind(self):
        if self.fsm.isstate('binding'):
            self.fsm.no_bind()

    def add_halrcomp_topic(self, name):
        self.halrcomp_channel.add_socket_topic(name)

    def remove_halrcomp_topic(self, name):
        self.halrcomp_channel.remove_socket_topic(name)

    def clear_halrcomp_topics(self):
        self.halrcomp_channel.clear_socket_topics()

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

    def start_halrcmd_channel(self):
        self.halrcmd_channel.start()

    def stop_halrcmd_channel(self):
        self.halrcmd_channel.stop()

    def start_halrcomp_channel(self):
        self.halrcomp_channel.start()

    def stop_halrcomp_channel(self):
        self.halrcomp_channel.stop()

    # process all messages received on halrcmd
    def halrcmd_channel_message_received(self, rx):

        # react to halrcomp bind confirm message
        if rx.type == pb.MT_HALRCOMP_BIND_CONFIRM:
            if self.fsm.isstate('binding'):
                self.fsm.bind_confirmed()

        # react to halrcomp bind reject message
        if rx.type == pb.MT_HALRCOMP_BIND_REJECT:
            # update error string with note
            self.error_string = ''
            for note in rx.note:
                self.error_string += note + '\n'
            if self.fsm.isstate('binding'):
                self.fsm.bind_rejected()

        # react to halrcomp set reject message
        if rx.type == pb.MT_HALRCOMP_SET_REJECT:
            # update error string with note
            self.error_string = ''
            for note in rx.note:
                self.error_string += note + '\n'
            if self.fsm.isstate('synced'):
                self.fsm.set_rejected()

        if self.halrcmd_message_received_cb:
            self.halrcmd_message_received_cb(rx)

    # process all messages received on halrcomp
    def halrcomp_channel_message_received(self, topic, rx):

        # react to halrcomp full update message
        if rx.type == pb.MT_HALRCOMP_FULL_UPDATE:
            self.halrcomp_full_update_received(topic, rx)

        # react to halrcomp incremental update message
        if rx.type == pb.MT_HALRCOMP_INCREMENTAL_UPDATE:
            self.halrcomp_incremental_update_received(topic, rx)

        # react to halrcomp error message
        if rx.type == pb.MT_HALRCOMP_ERROR:
            # update error string with note
            self.error_string = ''
            for note in rx.note:
                self.error_string += note + '\n'
            if self.fsm.isstate('syncing'):
                self.fsm.sync_failed()
            self.halrcomp_error_received(topic, rx)

        if self.halrcomp_message_received_cb:
            self.halrcomp_message_received_cb(topic, rx)

    def halrcomp_full_update_received(self, topic, rx):
        print('SLOT halrcomp full update unimplemented')

    def halrcomp_incremental_update_received(self, topic, rx):
        print('SLOT halrcomp incremental update unimplemented')

    def halrcomp_error_received(self, topic, rx):
        print('SLOT halrcomp error unimplemented')

    def send_halrcmd_message(self, msg_type, tx):
        self.halrcmd_channel.send_socket_message(msg_type, tx)

    def send_halrcomp_bind(self, tx):
        self.send_halrcmd_message(pb.MT_HALRCOMP_BIND, tx)

    def send_halrcomp_set(self, tx):
        self.send_halrcmd_message(pb.MT_HALRCOMP_SET, tx)

    def halrcmd_channel_state_changed(self, state):

        if (state == 'trying'):
            if self.fsm.isstate('syncing'):
                self.fsm.halrcmd_trying()
            if self.fsm.isstate('synced'):
                self.fsm.halrcmd_trying()
            if self.fsm.isstate('binding'):
                self.fsm.halrcmd_trying()

        if (state == 'up'):
            if self.fsm.isstate('trying'):
                self.fsm.halrcmd_up()

    def halrcomp_channel_state_changed(self, state):

        if (state == 'trying'):
            if self.fsm.isstate('synced'):
                self.fsm.halrcomp_trying()

        if (state == 'up'):
            if self.fsm.isstate('syncing'):
                self.fsm.halrcomp_up()

