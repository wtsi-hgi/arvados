import unittest
from abc import ABCMeta, abstractmethod

import lmdb

from arvados.keepcache._value_managers import InMemoryValueManager, LMDBValueManager
from tests.keepcache._common import TempManager


class _TestValueManager(unittest.TestCase):
    """
    Tests for `ValueManager`.
    """
    __metaclass__ = ABCMeta

    def setUp(self):
        self.value_manager = self._create_value_manager()

    @abstractmethod
    def _create_value_manager(self):
        """
        Creates a manager to test with.
        :return: the created manager
        :rtype: ValueManager
        """

    def test_get_highest_value(self):
        self.value_manager.add_value(3.0, "a")
        self.value_manager.add_value(-4.0, "b")
        self.value_manager.add_value(2.9, "c")
        self.assertEqual(3.0, self.value_manager.get_highest_value())

    def test_get_highest_value_when_no_value(self):
        self.assertIsNone(self.value_manager.get_highest_value())

    def test_get_highest_value_when_multiple_same_values(self):
        self.value_manager.add_value(3.0, "a")
        self.value_manager.add_value(3.0, "b")
        self.assertEqual(3.0, self.value_manager.get_highest_value())

    def test_add_value_as_inf(self):
        self.value_manager.add_value(float("inf"), "a")
        self.assertEqual(float("inf"), self.value_manager.get_highest_value())

    def test_add_value_overrides_old(self):
        self.value_manager.add_value(2.0, "a")
        self.value_manager.add_value(1.0, "a")
        self.assertEqual(1.0, self.value_manager.get_highest_value())

    def test_remove_value(self):
        self.value_manager.add_value(4.0, "a")
        self.value_manager.add_value(3.0, "b")
        self.value_manager.remove_value("a")
        self.assertEqual(3.0, self.value_manager.get_highest_value())

    def test_remove_none_existent_value(self):
        self.value_manager.remove_value("c")


class TestInMemoryValueManager(_TestValueManager):
    """
    Tests for `InMemoryValueManager`.
    """
    def _create_value_manager(self):
        return InMemoryValueManager()


class TestLMDBValueManager(_TestValueManager):
    """
    Tests for `LMDBValueManager`.
    """
    def setUp(self):
        self._temp_manager = TempManager()
        super(TestLMDBValueManager, self).setUp()

    def tearDown(self):
        self._temp_manager.remove_all()
        super(TestLMDBValueManager, self).tearDown()

    def _create_value_manager(self):
        directory = self._temp_manager.create_directory()
        environment = lmdb.open(directory)
        return LMDBValueManager(environment)


# Stop unittest from trying to instantiate the abstract base class as a test
del _TestValueManager
