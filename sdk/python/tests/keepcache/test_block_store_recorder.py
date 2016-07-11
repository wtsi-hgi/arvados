import os
import tempfile
import unittest
from abc import ABCMeta, abstractmethod

from arvados.keepcache.block_store_recorder import DatabaseBlockStoreUsageRecorder

_LOCATORS = ["123", "456", "789"]


class TestBlockStoreUsageRecorder(unittest.TestCase):
    """
    Unit tests for `BlockStoreUsageRecorder`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_recorder(self):
        """
        Creates a record that is to be tested.
        :return: the recorder
        :rtype: BlockStoreUsageRecorder
        """

    def setUp(self):
        self.recorder = self.create_recorder()

    def test_get_size(self):
        self.recorder.record_put("1", 1)
        for _ in range(10):
            for i in range(5):
                locator = "deleted_%s" % i
                self.recorder.record_put(locator, 99)
                self.recorder.record_delete(locator)
        self.recorder.record_put("2", 99)
        self.recorder.record_delete("2")
        self.recorder.record_put("2", 2)
        self.recorder.record_put("3", 3)
        self.assertEqual(6, self.recorder.get_size())

    def test_record_get(self):
        for locator in _LOCATORS:
            self.recorder.record_get(locator)
        records = self.recorder.get_all_records()
        self.assertEqual(set(_LOCATORS), {record.locator for record in records})

    def test_record_put(self):
        locator_size_pairs = [(_LOCATORS[i], i) for i in range(len(_LOCATORS))]
        for locator, size in iter(locator_size_pairs):
            self.recorder.record_put(locator, size)
        records = self.recorder.get_all_records()
        self.assertEqual(set(locator_size_pairs),
                         {(record.locator, record.size) for record in records})

    def test_record_delete(self):
        for locator in _LOCATORS:
            self.recorder.record_delete(locator)
        records = self.recorder.get_all_records()
        self.assertEqual(set(_LOCATORS), {record.locator for record in records})


class TestDatabaseBlockStoreUsageRecorder(TestBlockStoreUsageRecorder):
    """
    Tests for `DatabaseBlockStoreUsageRecorder`.
    """
    def setUp(self):
        self._database_locations = []   # type: List[str]
        super(TestDatabaseBlockStoreUsageRecorder, self).setUp()

    def tearDown(self):
        for location in self._database_locations:
            os.remove(location)

    def create_recorder(self):
        _, database_location = tempfile.mkstemp()
        self._database_locations.append(database_location)
        return DatabaseBlockStoreUsageRecorder(
            "sqlite:///%s" % database_location)


# Work around to stop unittest from trying to run the abstract base class
del TestBlockStoreUsageRecorder
