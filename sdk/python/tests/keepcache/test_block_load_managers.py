import unittest
from abc import ABCMeta, abstractmethod

import lmdb

from arvados.keepcache.block_load_managers import LMDBBlockLoadManager, \
    InMemoryBlockLoadManager
from tests.keepcache._common import LOCATOR_1, TempManager, LOCATOR_2, \
    get_superclass


class _TestBlockLoadManager(unittest.TestCase):
    """
    Tests for `BlockLoadManager`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_block_load_manager(self, timeout=float("inf")):
        """
        Creates a block load manager to test with.
        :param timeout:
        :return: the created manager
        :rtype: BlockLoadManager
        """

    def setUp(self):
        self.block_load_manager = self._create_block_load_manager()

    def test_reserve_load_rights_when_not_reserved(self):
        reserved = self.block_load_manager.reserve_load_rights(LOCATOR_1)
        self.assertTrue(reserved)

    def test_reserve_load_rights_when_already_reserved(self):
        self.block_load_manager.reserve_load_rights(LOCATOR_1)
        reserved = self.block_load_manager.reserve_load_rights(LOCATOR_1)
        self.assertFalse(reserved)

    def test_relinquish_load_rights(self):
        self.block_load_manager.reserve_load_rights(LOCATOR_1)
        assert LOCATOR_1 in self.block_load_manager.pending
        self.block_load_manager.relinquish_load_rights(LOCATOR_1)
        self.assertNotIn(LOCATOR_1, self.block_load_manager.pending)

    def test_pending(self):
        self.block_load_manager.reserve_load_rights(LOCATOR_1)
        self.block_load_manager.reserve_load_rights(LOCATOR_2)
        self.assertSetEqual({LOCATOR_1, LOCATOR_2}, self.block_load_manager.pending)


class _TestMultiProcessSafeBlockLoadManager(_TestBlockLoadManager):
    """
    Tests for a block load manager that can be "used" by multiple processes
    concurrently.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_connected_block_load_manager(self, block_manager):
        """
        Creates a block manager that is connected to the same data source as
        the given manager.
        :param block_manager: the manager that the new one is to be used
        concurrently with
        :return: the created block block load manager
        :rtype: BlockLoadManager
        """

    def setUp(self):
        # XXX: This `super` stupidness is caused by the solution to unittest
        # wanting to test abstract classes and Python 2 not supporting
        #`super()`.
        get_superclass(type(self), "_TestBlockLoadManager").setUp(self)
        self.block_load_manager_1 = self.block_load_manager
        self.block_load_manager_2 = self._create_connected_block_load_manager(
            self.block_load_manager_1)

    def test_set_global_timeout(self):
        max_load_timeout = 10.0
        self.block_load_manager_1.timeout = max_load_timeout / 2
        self.block_load_manager_2.timeout = max_load_timeout
        self.assertEqual(max_load_timeout, self.block_load_manager_1.global_timeout)
        self.assertEqual(max_load_timeout, self.block_load_manager_1.global_timeout)

    def test_pending_with_local_timeout(self):
        self.block_load_manager_2.timeout = 100.0
        self.block_load_manager_1.timeout = 10.0

        self.block_load_manager_1.get_time = lambda: 0.0
        self.block_load_manager_1.reserve_load_rights(LOCATOR_1)
        self.block_load_manager_1.get_time = lambda: 9.0
        self.assertIn(LOCATOR_1, self.block_load_manager_1.pending)
        self.block_load_manager_1.get_time = lambda: 11.0

        self.assertNotIn(LOCATOR_1, self.block_load_manager_1.pending)
        self.assertIn(LOCATOR_1, self.block_load_manager_1._get_pending_loads())

    def test_pending_with_global_timeout(self):
        self.block_load_manager_2.timeout = 1.0
        self.block_load_manager_1.timeout = 10.0

        self.block_load_manager_1.get_time = lambda: 0.0
        self.block_load_manager_1.reserve_load_rights(LOCATOR_1)
        self.block_load_manager_1.get_time = lambda: 9.0
        self.assertIn(LOCATOR_1, self.block_load_manager_1.pending)
        self.block_load_manager_1.get_time = lambda: 11.0

        self.assertNotIn(LOCATOR_1, self.block_load_manager_1.pending)
        self.assertNotIn(LOCATOR_1, self.block_load_manager_1._get_pending_loads())


class TestInMemoryBlockLoadManager(_TestBlockLoadManager):
    """
    Tests for `InMemoryBlockLoadManager`.
    """
    def _create_block_load_manager(self, timeout=float("inf")):
        return InMemoryBlockLoadManager(timeout=timeout)


class TestLMDBBlockLoadManager(_TestMultiProcessSafeBlockLoadManager):
    """
    Tests for `LMDBBlockLoadManager`.
    """
    def setUp(self):
        self._temp_manager = TempManager()
        self._manager_directory_map = dict()    # type: Dict[LMDBBlockLoadManager, str]
        super(TestLMDBBlockLoadManager, self).setUp()

    def tearDown(self):
        self._temp_manager.remove_all()
        super(TestLMDBBlockLoadManager, self).tearDown()

    def _create_block_load_manager(self, timeout=float("inf")):
        directory = self._temp_manager.create_directory()
        return self._create_block_load_manager_using_directory(directory, timeout)

    def _create_connected_block_load_manager(self, block_manager):
        directory = self._manager_directory_map[block_manager]
        timeout = block_manager.timeout
        return self._create_block_load_manager_using_directory(directory, timeout)

    def _create_block_load_manager_using_directory(self, directory, timeout):
        """
        TODO
        :param directory:
        :param timeout:
        :return:
        """
        environment = lmdb.open(directory, max_dbs=2)
        block_manager = LMDBBlockLoadManager(environment, timeout=timeout)
        self._manager_directory_map[block_manager] = directory
        return block_manager



# Stop unittest from trying to instantiate the abstract base classes as a tests
del _TestBlockLoadManager
del _TestMultiProcessSafeBlockLoadManager


if __name__ == "__main__":
    unittest.main()
