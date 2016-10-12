import logging
import os
import threading
import uuid
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from copy import copy
from threading import RLock

from monotonic import monotonic

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
    def relinquish_load_rights(self, locator, complete=True):
        """
        Relinquishes the rights (if any) held by any process that has the
        rights to load the given locator.
        :param locator: the locator of the block to get load rights for
        :type locator: str
        :param complete: signals that the load was completed successfully
        :type complete: bool
        """

    @abstractmethod
    def _reserve_load_rights(self, locator, identifier):
        """
        Reserves exclusive rights to load the given locator into the block
        store. The return value indicates whether the rights were granted.
        :param locator: the locator of the block to be loaded
        :type locator: str
        :param identifier: TODO
        :type identifier: str
        :return: `True` if exclusive rights were given to load block, else
        `False` if another has already gained the rights
        :rtype: bool
        """

    @abstractmethod
    def _get_pending_loads(self):
        """
        Gets the loads that are pending.
        :return: the pending loads
        :rtype: Dict[str, str]
        """

    def __init__(self, global_timeout_manager, timeout=float("inf")):
        """
        Constructor.
        :param global_timeout_manager: TODO
        :type global_timeout_manager: ValueManager
        :param timeout: TODO
        :type timeout: float
        """
        self.identifier = "%s-%s-%s" % (os.getpid(),
                                        threading.current_thread(),
                                        str(uuid.uuid4()))
        self._global_timeout_manager = global_timeout_manager
        self._timeout = None
        self.timeout = timeout
        self._dated_pending_loads = OrderedDict()  # type: OrderedDict[str, Tuple[str, int]]

    def __del__(self):
        """
        Destructor.
        """
        self._global_timeout_manager.remove_value(self.identifier)

    @property
    def pending(self):
        """
        Gets the set of block loads that are pending completion.
        :return: the loads that are pending
        :rtype: Iterable[str]
        """
        pending = self._get_pending_loads()
        # Adds new load identifiers
        for locator, identifier in pending.iteritems():
            if identifier not in self._dated_pending_loads:
                self._dated_pending_loads[identifier] = (locator, self.get_time())
        # Removes removed load identifiers
        for identifier in self._dated_pending_loads.keys():
            if identifier not in pending.values():
                del self._dated_pending_loads[identifier]
        # Times out any old loads
        self._remove_timed_out_loads()
        return {data[0] for key, data in self._dated_pending_loads.iteritems()}

    @property
    def global_timeout(self):
        """
        TODO
        :return:
        """
        return self._global_timeout_manager.get_highest_value()

    @property
    def timeout(self):
        """
        TODO
        :return:
        """
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        """
        TODO
        :param value:
        :return:
        """
        self._timeout = value
        self._global_timeout_manager.add_value(value, self.identifier)

    def reserve_load_rights(self, locator):
        """
        TODO
        :param locator:
        :return:
        """
        identifier = "%d-%s" % (os.getpid(), uuid.uuid4())
        self._dated_pending_loads[identifier] = (locator, self.get_time())
        return self._reserve_load_rights(locator, identifier)

    def get_time(self):
        """
        Gets a monotonic time, expressed in seconds as a float. Only the
        difference between times should be used.
        :return: the current time
        :rtype: float
        """
        return monotonic()

    def _remove_timed_out_loads(self):
        """
        TODO
        """
        current_time = self.get_time()
        global_timeout = self.global_timeout
        # Exploits ordering of `OrderedDict`
        for identifier, data in self._dated_pending_loads.items():
            locator, seen_time = data
            if current_time - seen_time > self.timeout:
                if self.timeout == global_timeout:
                    self.relinquish_load_rights(locator, complete=False)
                # Using `items()` in Python 2, which does not return an
                # iterator so this does not interrupt the loop
                del self._dated_pending_loads[identifier]
            else:
                break


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
        super(InMemoryBlockLoadManager, self).__init__(global_timeout_manager, timeout)
        self._pending = dict()     # type: Dict[str, float]
        self._pending_lock = RLock()

    def relinquish_load_rights(self, locator, complete=True):
        with self._pending_lock:
            if locator in self._pending:
                del self._pending[locator]

    def _reserve_load_rights(self, locator, identifier):
        with self._pending_lock:
            if locator in self._pending:
                return False
            self._pending[locator] = identifier
            return True

    def _get_pending_loads(self):
        with self._pending_lock:
            return copy(self._pending)


class LMDBBlockLoadManager(BlockLoadManager):
    """
    TODO
    """
    _GLOBAL_TIMEOUT_DATABASE = "GlobalTimeout"

    def __init__(self, environment, database=None, timeout=float("inf")):
        """
        Constructor.
        :param environment: TODO
        :param database: TODO. Must not have any sub-databases
        :type database: str
        :param timeout: TODO
        """
        self._environment = environment
        self._database = environment.open_db(database) if isinstance(database, str) else None
        timeout_database = environment.open_db(LMDBBlockLoadManager._GLOBAL_TIMEOUT_DATABASE)
        global_timeout_manager = LMDBValueManager(environment, timeout_database)
        super(LMDBBlockLoadManager, self).__init__(global_timeout_manager, timeout)

    def relinquish_load_rights(self, locator, complete=True):
        if complete:
            _logger.info("Completed loading block associated to `%s`" % locator)
        else:
            _logger.info("Block `%s` no longer considered as being loaded" % locator)
        locator = to_bytes(locator)
        with self._environment.begin(write=True, db=self._database) as transaction:
            transaction.delete(locator)

    def _reserve_load_rights(self, locator, identifier):
        locator = to_bytes(locator)
        with self._environment.begin(write=True, db=self._database) as transaction:
            reserved = transaction.put(locator, identifier, overwrite=False)
            _logger.info("%s to reserve load rights for locator `%s`"
                         % ("Succeeded" if reserved else "Failed", locator))
            return reserved

    def _get_pending_loads(self):
        with self._environment.begin(db=self._database) as transaction:
            with transaction.cursor(self._database) as cursor:
                pending = {key: value for key, value in cursor}
                # Strange quirk of LMDB is that the names of sub-database are
                # keys
                del pending[LMDBBlockLoadManager._GLOBAL_TIMEOUT_DATABASE]
                return pending
