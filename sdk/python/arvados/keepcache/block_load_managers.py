import logging
import os
import time
import uuid
from abc import ABCMeta, abstractmethod
from copy import copy
from errno import EEXIST
from threading import RLock

import lmdb

from arvados.keepcache._common import to_bytes
from arvados.keepcache._value_managers import InMemoryValueManager, \
    LMDBValueManager

_logger = logging.getLogger(__name__)


class BlockLoadManager(object):
    """
    Manages the loading of blocks.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def relinquish_load_rights(self, locator, timestamp):
        """
        Relinquishes the rights (if any) of the loader for the given locator,
        started at the given timestamp.
        """

    @abstractmethod
    def _reserve_load_rights(self, locator, timestamp):
        """
        Reserves exclusive rights to load the given locator into the block
        store. The return value indicates whether the rights were granted.
        :param locator: the locator of the block to be loaded
        :type locator: str
        :param timestamp: time when load was started in milliseconds from epoch
        :type timestamp: int
        :return: timestamp of the reserve else `None` if not reserved
        :rtype: Optional[float]
        """

    @abstractmethod
    def _read_load_rights(self):
        """
        Gets the loads rights that are currently active.
        :return: dictionary indexed by locator of the timestamps of when rights
        were granted
        :rtype: Dict[str, int]
        """

    def __init__(self, global_timeout_manager, timeout=float("inf")):
        """
        Constructor.
        :param global_timeout_manager: manager of global load timeouts
        :type global_timeout_manager: ValueManager
        :param timeout: the number of seconds before a load is considered to
        have timed out
        :type timeout: float
        """
        self.identifier = "%s-%s" % (os.getpid(), str(uuid.uuid4()))
        self._global_timeout_manager = global_timeout_manager
        self._timeout = None
        self.timeout = timeout

    def __del__(self):
        """
        Destructor.
        """
        # Beware of:
        # http://www.algorithm.co.il/blogs/programming/python-gotchas-1-__del__-is-not-the-opposite-of-__init__/
        try:
            self._global_timeout_manager.remove_value(self.identifier)
        except AttributeError:
            """ The constructor did not complete """

    @property
    def pending(self):
        """
        Gets the set of block loads that are still valid and pending
        completion. Use of this accessor triggers management of the pending
        list (old rights may be removed).
        :return: the loads that are pending
        :rtype: Set[str]
        """
        pending = self._read_load_rights()
        current_time = self.get_time()
        global_timeout = self.global_timeout

        active = set()  # type: Set[str]
        for locator, timestamp in pending.iteritems():
            if current_time < timestamp + self.timeout:
                active.add(locator)
            elif self.timeout == global_timeout:
                self.relinquish_load_rights(locator, timestamp)
        return active

    @property
    def global_timeout(self):
        """
        Gets the global timeout, defined as the longest timeout of any
        block load manager using the same data source.
        :return: global timeout in seconds
        :rtype: float
        """
        return self._global_timeout_manager.get_highest_value()

    @property
    def timeout(self):
        """
        Gets the timeout this block load manager uses to decide if the process
        currently listed as loading a block has timed out.
        :return: the timeout in seconds
        :rtype: float
        """
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        """
        Sets the timeout.
        :param value: timeout in seconds
        :type value: float
        """
        self._timeout = value
        self._global_timeout_manager.add_value(value, self.identifier)

    def reserve_load_rights(self, locator):
        """
        Reserves the right for this block load manager to load the block with
        the given locator.
        :param locator: the locator of the block to load
        :type locator: str
        :return: timestamp of the reserve else `None` if not reserved
        :rtype: Optional[float]
        """
        timestamp = self.get_time()
        return self._reserve_load_rights(locator, timestamp)

    def get_time(self):
        """
        Gets the number of milliseconds since the epoch. Must be synchronised
        with other processes.
        :return: milliseconds since the epoch
        :rtype: int
        """
        return int(time.time() * 1000)


class InMemoryBlockLoadManager(BlockLoadManager):
    """
    In-memory block load manager.

    Given that this manager only uses local memory, it will not work across
    processes. It is however thread-safe.
    """
    def __init__(self, timeout=float("inf")):
        """
        Constructor.
        :param timeout: load timeout in seconds
        :type timeout: float
        """
        global_timeout_manager = InMemoryValueManager()
        super(InMemoryBlockLoadManager, self).__init__(
            global_timeout_manager, timeout)
        self._pending = dict()     # type: Dict[str, int]
        self._pending_lock = RLock()

    def relinquish_load_rights(self, locator, timestamp):
        with self._pending_lock:
            if locator in self._pending:
                del self._pending[locator]

    def _reserve_load_rights(self, locator, timestamp):
        with self._pending_lock:
            if locator in self._pending:
                return None
            self._pending[locator] = timestamp
            return timestamp

    def _read_load_rights(self):
        return copy(self._pending)


class LMDBBlockLoadManager(BlockLoadManager):
    """
    LMDB backed block load manager.

    This can be setup to work across multiple processes.
    """
    _PENDING_DATABASE = "Pending"
    _GLOBAL_TIMEOUT_DATABASE = "GlobalTimeout"

    def __init__(self, environment, timeout=float("inf")):
        """
        Constructor.
        :param environment: the LMDB environment or path to the environment
        directory. If the former is given, it must be opened with support for 2
        databases
        :type environment: Union[Environment, str]
        :param timeout: the time in seconds before it is considered that a
        process loading a block has stopped
        :type timeout: float
        """
        if isinstance(environment, str) and not os.path.exists(environment):
            try:
                os.mkdir(environment)
            except OSError as e:
                if e.errno != EEXIST:
                    raise e
        self._environment = lmdb.open(environment, max_dbs=2) if isinstance(environment, str) else environment
        self._pending_database = self._environment.open_db(LMDBBlockLoadManager._PENDING_DATABASE)
        timeout_database = self._environment.open_db(LMDBBlockLoadManager._GLOBAL_TIMEOUT_DATABASE)
        global_timeout_manager = LMDBValueManager(self._environment, timeout_database)
        super(LMDBBlockLoadManager, self).__init__(global_timeout_manager, timeout)

    def relinquish_load_rights(self, locator, timestamp):
        locator = to_bytes(locator)
        assert isinstance(timestamp, int)
        timestamp = str(timestamp)
        with self._environment.begin(write=True, db=self._pending_database) as transaction:
            value = transaction.get(locator)
            if value == timestamp:
                _logger.info("Relinquishing load rights for locator `%s` with "
                             "timestamp of %s" % (locator, timestamp))
                transaction.delete(locator)

    def _reserve_load_rights(self, locator, timestamp):
        locator = to_bytes(locator)
        with self._environment.begin(write=True, db=self._pending_database) as transaction:
            reserved = transaction.put(locator, str(timestamp), overwrite=False)
            _logger.info("%s to reserve load rights for locator `%s`"
                         % ("Succeeded" if reserved else "Failed", locator))
            return timestamp if reserved else None

    def _read_load_rights(self):
        with self._environment.begin(db=self._pending_database) as transaction:
            with transaction.cursor(self._pending_database) as cursor:
                return {key: int(value) for key, value in cursor}
