from abc import ABCMeta, abstractmethod

from arvados.keepcache._common import to_bytes


class ValueManager(object):
    """
    Manages a set of values.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_highest_value(self):
        """
        Gets the current highest value. Agnostic to changes to values that may
        be ongoing. `None` if no values.
        :return: the current highest value
        :rtype: Optional[float]
        """

    @abstractmethod
    def add_value(self, value, identifier):
        """
        Adds the given value, associated to the given identifier. Will override
        previous values added with the same identifier.
        :param value: the value
        :type value: float
        :param identifier: identifier associated to the value to be added
        :type identifier: str
        """

    @abstractmethod
    def remove_value(self, identifier):
        """
        Removes the value (if any) associated to the given identifier.
        :param identifier: the identifier associated to the value to remove
        :type identifier: str
        """


class InMemoryValueManager(ValueManager):
    """
    In memory value manager.
    """
    def __init__(self):
        """
        Constructor.
        """
        # XXX: The computer scientist in me is not amused by use of this data
        # structure... However, adding a dependency on a library providing a
        # better structure cannot justified for the purpose of  making an
        # implementation (which will probably just be used in testing) more
        # efficient
        self._values = dict()

    def remove_value(self, identifier):
        self._values.pop(identifier, None)

    def get_highest_value(self):
        max = None
        for value in self._values.values():
            if max is None or value > max:
                max = value
        return max

    def add_value(self, value, identifier):
        self._values[identifier] = value


class LMDBValueManager(ValueManager):
    """
    LMDB backed value manager.
    """
    def __init__(self, environment, database=None):
        """
        Constructor.
        :param environment: the LMDB environment
        :type environment: Environment
        :param database: the LMDB sub-database to use (if any)
        :type database: Optional[Union[handle, str]]
        """
        self._environment = environment
        self._database_handle = environment.open(database) \
            if isinstance(database, str) else database

    def remove_value(self, identifier):
        identifier = to_bytes(identifier)
        with self._environment.begin(write=True, db=self._database_handle) \
                as transaction:
            transaction.delete(identifier)

    def get_highest_value(self):
        with self._environment.begin(db=self._database_handle) as transaction:
            with transaction.cursor(self._database_handle) as cursor:
                max = None
                for value in cursor.iternext(keys=False):
                    value = float(value)
                    if max is None or value > max:
                        max = value
                return max

    def add_value(self, value, identifier):
        identifier = to_bytes(identifier)
        with self._environment.begin(write=True, db=self._database_handle) \
                as transaction:
            transaction.put(identifier, str(value))
