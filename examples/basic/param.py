#!/usr/bin/env python

import time
import sys
import gobject
import threading

from pymachinetalk.dns_sd import ServiceDiscovery
from pymachinetalk import param


class BasicClass():
    def __init__(self):
        launcher_sd = ServiceDiscovery(service_type="_launcher._sub._machinekit._tcp")
        launcher_sd.on_discovered.append(self.service_discovered)
        launcher_sd.on_disappeared.append(self.service_disappeared)
        launcher_sd.start()
        self.launcher_sd = launcher_sd

        self.paramReady = False
        self.paramcmdReady = False
        self.param = param.ParamClient('system/testus', debug=True)

    def start_sd(self, uuid):
        param_sd = ServiceDiscovery(service_type="_param._sub._machinekit._tcp", uuid=uuid)
        param_sd.on_discovered.append(self.param_discovered)
        param_sd.start()
        #param_sd.disappered_callback = disappeared
        #self.param_sd = param_sd

        paramcmd_sd = ServiceDiscovery(service_type="_paramcmd._sub._machinekit._tcp", uuid=uuid)
        paramcmd_sd.on_discovered.append(self.paramcmd_discovered)
        paramcmd_sd.start()
        #self.harcomp_sd = paramcmd_sd

    def service_disappeared(self, data):
        print("disappeared %s %s" % (data.name))

    def service_discovered(self, data):
        print("discovered %s %s %s" % (data.name, data.dsn, data.uuid))
        self.start_sd(data.uuid)

    def param_discovered(self, data):
        print("discovered %s %s" % (data.name, data.dsn))
        self.param.param_uri = data.dsn
        self.paramReady = True
        if self.paramcmdReady:
            self.start_param()

    def paramcmd_discovered(self, data):
        print("discovered %s %s" % (data.name, data.dsn))
        self.param.paramcmd_uri = data.dsn
        self.paramcmdReady = True
        if self.paramReady:
            self.start_param()

    def start_param(self):
        print('connecting param client %s' % self.param.basekey)
        self.param.ready()

    def stop(self):
        self.param.stop()


def main():
    gobject.threads_init()  # important: initialize threads if gobject main loop is used
    basic = BasicClass()
    loop = gobject.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        loop.quit()

    print("stopping threads")
    basic.stop()

    # wait for all threads to terminate
    while threading.active_count() > 1:
        time.sleep(0.1)

    print("threads stopped")
    sys.exit(0)

if __name__ == "__main__":
    main()
