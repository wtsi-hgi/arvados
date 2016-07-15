import os
import tempfile
import unittest
from abc import ABCMeta, abstractmethod

from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper, SqlBlockStoreBookkeeper
from tests.keepcache._common import LOCATORS


class TestBlockStoreBookkeeper(unittest.TestCase):
    """
    Unit tests for `BlockStoreBookkeeper`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_bookkeeper(self):
        """
        Creates a bookkeeper of the type that is being tested.
        :return: the bookkeeper
        :rtype: BlockStoreBookkeeper
        """

    def setUp(self):
        self.bookkeeper = self._create_bookkeeper()

    def test_get_size(self):
        self.bookkeeper.record_put("1", 1)
        for _ in range(10):
            for i in range(5):
                locator = "deleted_%s" % i
                self.bookkeeper.record_put(locator, 99)
                self.bookkeeper.record_delete(locator)
        self.bookkeeper.record_put("2", 99)
        self.bookkeeper.record_delete("2")
        self.bookkeeper.record_put("2", 2)
        self.bookkeeper.record_put("3", 3)
        self.assertEqual(6, self.bookkeeper.get_size())

    def test_record_get(self):
        for locator in LOCATORS:
            self.bookkeeper.record_get(locator)
        records = self.bookkeeper.get_all_records()
        self.assertEqual(set(LOCATORS), {record.locator for record in records})

    def test_record_put(self):
        locator_size_pairs = [(LOCATORS[i], i) for i in range(len(LOCATORS))]
        for locator, size in iter(locator_size_pairs):
            self.bookkeeper.record_put(locator, size)
        records = self.bookkeeper.get_all_records()
        self.assertEqual(set(locator_size_pairs),
                         {(record.locator, record.size) for record in records})

    def test_record_delete(self):
        for locator in LOCATORS:
            self.bookkeeper.record_delete(locator)
        records = self.bookkeeper.get_all_records()
        self.assertEqual(set(LOCATORS), {record.locator for record in records})


class TestInMemoryBlockStoreBookkeeper(TestBlockStoreBookkeeper):
    """
    Tests for `InMemoryBlockStoreBookkeeper`.
    """
    def _create_bookkeeper(self):
        return InMemoryBlockStoreBookkeeper()


class TestSqlBlockStoreBookkeeper(TestBlockStoreBookkeeper):
    """
    Tests for `SqlBlockStoreBookkeeper`.
    """
    def setUp(self):
        self._database_locations = []   # type: List[str]
        super(TestSqlBlockStoreBookkeeper, self).setUp()

    def tearDown(self):
        for location in self._database_locations:
            os.remove(location)

    def _create_bookkeeper(self):
        _, database_location = tempfile.mkstemp()
        self._database_locations.append(database_location)
        return SqlBlockStoreBookkeeper("sqlite:///%s" % database_location)


# Work around to stop unittest from trying to run the abstract base class
del TestBlockStoreBookkeeper
