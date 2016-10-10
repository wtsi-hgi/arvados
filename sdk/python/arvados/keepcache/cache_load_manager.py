import os
from abc import ABCMeta, abstractmethod, abstractproperty
from time import sleep

import lmdb

from arvados.keepcache._common import to_bytes


class CacheLoadManager(object):
    """
    Manages the loading of blocks into the cache to remove simultaneous loading
    of the same block by multiple processes.
    """
    __metaclass__ = ABCMeta

    class AlreadyLoadingError(Exception):
        """
        Error indicating that an action cannot be completed because a block is
        already being loaded into the cache.
        """

    @abstractproperty
    def pending_loads(self):
        """
        Gets the set of block loads that are pending completion by other
        processes.
        :return: the loads that are pending
        :rtype: Set[str]
        """

    @abstractmethod
    def reserve_load(self, locator):
        """
        Reserves exclusive rights to load the given locator into the cache. The
        return value indicates whether the rights were granted.

        Process-safe.
        :param locator: the locator of the block to be loaded
        :type locator: str
        :return: `True` if exclusive rights were given to load block, else
        `False` if another process has already gained the rights
        :rtype: bool
        """

    @abstractmethod
    def load_completed(self, locator):
        """
        TODO
        :param locator:
        :type locator: str
        :return:
        :rtype: str
        """

    @abstractmethod
    def wait_for_load(self, locator, timeout):
        """
        Waits for the block with the given locator to be loaded into the cache.
        Gives up after `timeout` seconds and will return `False` to indicate
        that the load is not going to complete.

        Beware that just because the block was loaded into the cache, there is
        no guarantee that it will still be there when access is attempted.

        Process-safe.
        :param locator: the block locator
        :type locator: str
        :param timeout: seconds before wait times out
        :type timeout: float
        :return: `True` if the load completed else `False` if the load is not
        going to complete
        :rtype: bool
        """


class LMDBCacheLoadManager(CacheLoadManager):
    """
    TODO
    """
    SUB_DATABASE_NAME = "cacheLoadManager"
    DEFAULT_POLL_PERIOD = 0.5

    def __init__(self, directory, poll_period=DEFAULT_POLL_PERIOD):
        """
        Constructor.
        :param directory: the directory of the LMDB database to use
        :type directory: str
        :param block_store: TODO
        :type block_store: BlockStore
        :param poll_period: TODO
        :type poll_period: float
        """
        # FIXME: The consequences of using a sub-director on max size
        # calculations has not been considered!
        self._lmdb_environment = lmdb.open(directory, max_dbs=2)
        self._loading_database = self._lmdb_environment.open_db(
            LMDBCacheLoadManager.SUB_DATABASE_NAME)
        self.poll_period = poll_period

    @property
    def pending_loads(self):
        with self._lmdb_environment.begin(db=self._loading_database) as transaction:
            with transaction.cursor(self._loading_database) as cursor:
                return {key for key in cursor.iternext(values=False)}

    def reserve_load(self, locator):
        locator = to_bytes(locator)
        with self._lmdb_environment.begin(write=True, db=self._loading_database) as transaction:
            return transaction.put(locator, bytes(os.getpid()), overwrite=False)

    def load_completed(self, locator):
        locator = to_bytes(locator)
        with self._lmdb_environment.begin(write=True, db=self._loading_database) as transaction:
            deleted = transaction.delete(locator)
            if not deleted:
                raise ValueError("No reserve for locator `%s` found" % locator)

    def wait_for_load(self, locator, timeout=float("inf")):
        waited_for = 0
        while True:
            if locator not in self.pending_loads:
                return True
            if waited_for >= timeout:
                # TODO: Consider adding "evidence" to suggest current loader
                # has stopped
                return False
            sleep(self.poll_period)
            waited_for += self.poll_period
