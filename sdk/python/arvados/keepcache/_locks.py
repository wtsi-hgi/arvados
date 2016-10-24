from threading import Lock

from filelock import FileLock


class GlobalLock(object):
    """
    Lock that applies across processes.
    """
    def __init__(self, location):
        """
        Constructor.
        :param location: location of the lock file
        :type location: str
        """
        self._file_lock = FileLock(location)
        self._thread_lock = Lock()

    def acquire(self):
        """
        Acquires the lock.
        """
        self._thread_lock.acquire()
        self._file_lock.acquire()

    def release(self):
        """
        Releases the lock.
        """
        self._file_lock.release()
        self._thread_lock.release()

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()
