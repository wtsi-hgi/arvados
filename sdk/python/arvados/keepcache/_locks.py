from threading import Lock

from filelock import FileLock


class ThreadAndProcessLock(object):
    """
    TODO
    """
    def __init__(self, location):
        """
        TODO
        :param location:
        """
        self._file_lock = FileLock(location)
        self._thread_lock = Lock()

    def acquire(self):
        self._thread_lock.acquire()
        self._file_lock.acquire()

    def release(self):
        self._file_lock.release()
        self._thread_lock.release()

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()
