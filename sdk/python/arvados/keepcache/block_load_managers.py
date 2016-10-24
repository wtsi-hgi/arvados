import logging
import os
import threading
import uuid
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from copy import copy
from threading import RLock

import lmdb
from bidict import bidict
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
    def relinquish_load_rights(self, identifier):
        """
        Relinquishes the rights (if any) corresponding to the given identifier.
        :param identifier: the load identifier
        :type identifier: str
        """

    @abstractmethod
    def _reserve_load_rights(self, locator, identifier):
        """
        Reserves exclusive rights to load the given locator into the block
        store. The return value indicates whether the rights were granted.
        :param locator: the locator of the block to be loaded
        :type locator: str
        :param identifier: identifier of block loader
        :type identifier: str
        :return: `True` if exclusive rights were given to load block, else
        `False` if another has already gained the rights
        :rtype: bool
        """

    @abstractmethod
    def _read_load_rights(self):
        """
        Gets the loads rights that are currently active.
        :return: identifiers of loaders, indexed by the locator they are
        loading
        :rtype: Dict[str, str]
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
        # Beware of:
        # http://www.algorithm.co.il/blogs/programming/python-gotchas-1-__del__-is-not-the-opposite-of-__init__/
        try:
            # XXX: If this fails, stalled loads are potentially not going to be
            # removed from the global pending list. Every load manager will
            # then have to wait for their `timeout` for all stalled loads. The
            # negative impact could be reduced by switching to the use of
            # timestamps and assuming synchronised clocks.
            self._global_timeout_manager.remove_value(self.identifier)
        except AttributeError:
            pass

    @property
    def pending(self):
        """
        Gets the set of block loads that are still valid and pending
        completion. Use of this accessor triggers management of the global
        reseve table (old reservations may be removed)
        :return: the loads that are pending
        :rtype: Iterable[str]
        """
        pending = self._read_load_rights()
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
        TODO
        :param value:
        :return:
        """
        self._timeout = value
        self._global_timeout_manager.add_value(value, self.identifier)

    def reserve_load_rights(self, locator):
        """
        Reserves the right for this block load manager to load the block with
        the given locator.
        :param locator: the locator of the block to load
        :type locator: str
        :return: `True` if this load manager has been given the rights to load
        the block, else `False` if the block is to be loaded by another
        :rtype: Optional[str]
        """
        identifier = "%d-%s" % (os.getpid(), uuid.uuid4())
        self._dated_pending_loads[identifier] = (locator, self.get_time())
        load_rights = self._reserve_load_rights(locator, identifier)
        return identifier if load_rights else None

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
        Removes loads from `self._dated_pending_loads` that are considered to
        have timed out. If the timeout used by this block load manager is the
        highest of all load managers, the loads rights are removed from the
        current loader.
        """
        current_time = self.get_time()
        global_timeout = self.global_timeout
        # Exploits ordering of `OrderedDict`
        for identifier, data in self._dated_pending_loads.items():
            locator, seen_time = data
            if current_time - seen_time > self.timeout:
                if self.timeout == global_timeout:
                    self.relinquish_load_rights(identifier)
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
        super(InMemoryBlockLoadManager, self).__init__(
            global_timeout_manager, timeout)
        self._pending = bidict()
        self._pending_lock = RLock()

    def relinquish_load_rights(self, identifier):
        with self._pending_lock:
            if identifier in self._pending.inv:
                del self._pending.inv[identifier]

    def _reserve_load_rights(self, locator, identifier):
        with self._pending_lock:
            if locator in self._pending:
                return False
            self._pending[locator] = identifier
            return True

    def _read_load_rights(self):
        with self._pending_lock:
            return copy(self._pending)


class LMDBBlockLoadManager(BlockLoadManager):
    """
    LMDB backed block load manager.

    This can be setup to work across multiple processes.
    """
    _GLOBAL_TIMEOUT_DATABASE = "GlobalTimeout"

    def __init__(self, environment, database=None, timeout=float("inf")):
        """
        Constructor.
        :param environment: the LMDB environment or path to the environment
        directory. If the former is given, it must be opened with support for 2
        databases
        :type environment: Union[Environment, str]
        :param database: the database to use
        :type database: Union[handle, str]
        :param timeout: the time in seconds before it is considered that a
        process loading a block has stopped
        :type timeout: float
        """
        self._environment = lmdb.open(environment, max_dbs=2) if not isinstance(environment, lmdb.Environment) else environment
        self._database = self._environment.open_db(database) if isinstance(database, str) else database
        timeout_database = self._environment.open_db(LMDBBlockLoadManager._GLOBAL_TIMEOUT_DATABASE)
        global_timeout_manager = LMDBValueManager(self._environment, timeout_database)
        super(LMDBBlockLoadManager, self).__init__(global_timeout_manager, timeout)

    def relinquish_load_rights(self, identifier):
        with self._environment.begin(write=True, db=self._database) as transaction:
            with transaction.cursor(self._database) as cursor:
                for locator, value in cursor:
                    if value == identifier:
                        _logger.info("Relinquishing load rights with "
                                     "identifier `%s`" % locator)
                        transaction.delete(locator)
                        break

    def _reserve_load_rights(self, locator, identifier):
        locator = to_bytes(locator)
        with self._environment.begin(write=True, db=self._database) as transaction:
            reserved = transaction.put(locator, identifier, overwrite=False)
            _logger.info("%s to reserve load rights for locator `%s`"
                         % ("Succeeded" if reserved else "Failed", locator))
            return reserved

    def _read_load_rights(self):
        with self._environment.begin(db=self._database) as transaction:
            with transaction.cursor(self._database) as cursor:
                pending = {key: value for key, value in cursor}
                # Strange quirk of LMDB is that the names of sub-database are
                # keys
                del pending[LMDBBlockLoadManager._GLOBAL_TIMEOUT_DATABASE]
                return pending
