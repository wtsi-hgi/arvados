import sys
import time
import threading

class Stat(object):
    def __init__(self, prefix, interval,
                 egr_name, ing_name,
                 egr_func, ing_func):
        self.prefix = prefix
        self.interval = interval
        self.egr_name = egr_name
        self.ing_name = ing_name
        self.egress = egr_func
        self.ingress = ing_func
        self.egr_prev = self.egress()
        self.ing_prev = self.ingress()

    def update(self):
        egr = self.egress()
        ing = self.ingress()

        delta = " -- interval %.4f seconds %d %s %d %s" % (self.interval,
                                                           egr - self.egr_prev,
                                                           self.egr_name,
                                                           ing - self.ing_prev,
                                                           self.ing_name)

        sys.stderr.write("crunchstat: %s %d %s %d %s%s\n" % (self.prefix,
                                                             egr,
                                                             self.egr_name,
                                                             ing,
                                                             self.ing_name,
                                                             delta))

        self.egr_prev = egr
        self.ing_prev = ing


class StatLogger(threading.Thread):
    def __init__(self, interval, keep, ops):
        super(StatLogger, self).__init__()
        self._stop = threading.Event()
        self.name = "statlogger"
        self.interval = interval
        self.keep = keep
        self.ops = ops

    def stop(self):
        self._stop.set()

    def keep_running(self):
        return not self._stop.isSet()

    def run(self):
        calls = Stat("keepcalls", self.interval, "put", "get",
                     self.keep.put_counter.get,
                     self.keep.get_counter.get)
        net = Stat("net:keep0", self.interval, "tx", "rx",
                   self.keep.upload_counter.get,
                   self.keep.download_counter.get)
        cache = Stat("keepcache", self.interval, "hit", "miss",
                   self.keep.hits_counter.get,
                   self.keep.misses_counter.get)
        fuseops = Stat("fuseops", self.interval,"write", "read",
                       self.ops.write_ops_counter.get,
                       self.ops.read_ops_counter.get)
        blk = Stat("blkio:0:0", self.interval, "write", "read",
                   self.ops.write_counter.get,
                   self.ops.read_counter.get)

        while self.keep_running():
            time.sleep(self.interval)
            calls.update()
            net.update()
            cache.update()
            fuseops.update()
            blk.update()

