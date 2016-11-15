import unittest
from abc import ABCMeta, abstractmethod
from datetime import datetime

from mock import MagicMock

from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper, SqlBlockStoreBookkeeper, \
    LMDBBlockStoreBookkeeper
from arvados.keepcache.block_store_records import BlockGetRecord, \
    BlockPutRecord, BlockDeleteRecord
from tests.keepcache._common import CONTENTS, LOCATORS, TempManager, LOCATOR_1, \
    LOCATOR_2


class _TestBlockStoreBookkeeper(unittest.TestCase):
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

        # Make timestamps unqiue and deterministic
        def create_timestamp():
            return datetime(
                self.bookkeeper._get_current_timestamp.call_count, 1, 1)
        self.bookkeeper._get_current_timestamp = MagicMock(
            side_effect=create_timestamp)

    def test_get_active(self):
        self.bookkeeper.record_put("1", 1)
        self.bookkeeper.record_put("2", 2)
        self.bookkeeper.record_delete("1")
        active = self.bookkeeper.get_active()
        self.assertEqual(1, len(active))
        self.assertEqual("2", list(active)[0].locator)

    def test_get_active_storage_size(self):
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
        self.assertEqual(6, self.bookkeeper.get_active_storage_size())

    def test_get_all_get_records(self):
        self._test_get_all_records_of_type(
            self.bookkeeper.record_get, self.bookkeeper.get_all_get_records, BlockGetRecord)

    def test_get_all_get_records_with_locators_filter(self):
        self._test_get_all_records_of_type_with_locators_filter(
            self.bookkeeper.record_get, self.bookkeeper.get_all_get_records, BlockGetRecord)

    def test_get_all_get_records_with_since_filter(self):
        self._test_get_all_records_of_type_with_since_filter(
            self.bookkeeper.record_get, self.bookkeeper.get_all_get_records, BlockGetRecord)

    def test_get_all_put_records(self):
        def record_setter(locator):
            self.bookkeeper.record_put(locator, len(CONTENTS))

        self._test_get_all_records_of_type(
            record_setter, self.bookkeeper.get_all_put_records, BlockPutRecord)

    def test_get_all_put_records_with_locators_filter(self):
        def record_setter(locator):
            self.bookkeeper.record_put(locator, len(CONTENTS))

        self._test_get_all_records_of_type_with_locators_filter(
            record_setter, self.bookkeeper.get_all_put_records, BlockPutRecord)

    def test_get_all_put_records_with_since_filter(self):
        def record_setter(locator):
            self.bookkeeper.record_put(locator, len(CONTENTS))

        self._test_get_all_records_of_type_with_since_filter(
            record_setter, self.bookkeeper.get_all_put_records, BlockPutRecord)

    def test_get_all_delete_records(self):
        self._test_get_all_records_of_type(
            self.bookkeeper.record_delete, self.bookkeeper.get_all_delete_records, BlockDeleteRecord)

    def test_get_all_delete_records_with_locators_filter(self):
        self._test_get_all_records_of_type_with_locators_filter(
            self.bookkeeper.record_delete, self.bookkeeper.get_all_delete_records, BlockDeleteRecord)

    def test_get_all_delete_records_with_since_filter(self):
        self._test_get_all_records_of_type_with_since_filter(
            self.bookkeeper.record_delete, self.bookkeeper.get_all_delete_records, BlockDeleteRecord)

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

    def _test_get_all_records_of_type(
            self, record_setter, record_getter, record_type):
        """
        Tests get all records of a certain type.
        :param record_setter: adds record of the correct type in the bookkeeper,
        where the first argument is the locator
        :type record_type: Callable[[str], None]
        :param record_getter: gets all records of the correct type from the
        bookkeeper
        :type record_getter: Callable[[], Set[Records]]
        :param record_type: the type of record to get
        :rtype: type
        """
        for locator in LOCATORS:
            record_setter(locator)
        records = record_getter()
        self.assertEqual(set(LOCATORS), {record.locator for record in records})
        for record in records:
            self.assertIsInstance(record, record_type)

    def _test_get_all_records_of_type_with_locators_filter(
            self, record_setter, record_getter, record_type):
        """
        Tests get all records of a certain type with a filter on the locators of
        interest.
        :param record_setter: adds record of the correct type in the bookkeeper,
        where the first argument is the locator
        :type record_type: Callable[[str], None]
        :param record_getter: gets all records of the correct type from the
        bookkeeper
        :type record_getter: Callable[[], Set[Records]]
        :param record_type: the type of record to get
        :rtype: type
        """
        for locator in LOCATORS:
            record_setter(locator)
        records = record_getter(locators={LOCATORS[0]})
        self.assertEqual(1, len(records))
        record = list(records)[0]
        self.assertEqual(LOCATORS[0], record.locator)
        self.assertIsInstance(record, record_type)

    def _test_get_all_records_of_type_with_since_filter(
            self, record_setter, record_getter, record_type):
        """
        Tests get all records of a certain type with a filter on the timestamp
        of records.
        :param record_setter: adds record of the correct type in the bookkeeper,
        where the first argument is the locator
        :type record_type: Callable[[str], None]
        :param record_getter: gets all records of the correct type from the
        bookkeeper
        :type record_getter: Callable[[], Set[Records]]
        :param record_type: the type of record to get
        :rtype: type
        """
        for locator in LOCATORS:
            record_setter(locator)
        assert len({record.timestamp for record in record_getter()}) == len(LOCATORS), \
            "Records must have different timestamps"
        newest = max(record_getter(LOCATORS), key=lambda record: record.timestamp)
        records = record_getter(since=newest.timestamp)
        self.assertEqual(1, len(records))
        record = list(records)[0]
        self.assertEqual(newest.locator, record.locator)
        self.assertEqual(newest.timestamp, record.timestamp)
        self.assertIsInstance(record, record_type)


class TestInMemoryBlockStoreBookkeeper(_TestBlockStoreBookkeeper):
    """
    Tests for `InMemoryBlockStoreBookkeeper`.
    """
    def _create_bookkeeper(self):
        return InMemoryBlockStoreBookkeeper()


class TestSqlBlockStoreBookkeeper(_TestBlockStoreBookkeeper):
    """
    Tests for `SqlBlockStoreBookkeeper`.
    """
    def setUp(self):
        self._temp_directory_manager = TempManager()
        super(TestSqlBlockStoreBookkeeper, self).setUp()

    def tearDown(self):
        self._temp_directory_manager.remove_all()

    def _create_bookkeeper(self):
        database_location = self._temp_directory_manager.create_file()
        database_lock_location = "%s.lock" % database_location
        self._temp_directory_manager.temp_files.append(database_lock_location)
        return SqlBlockStoreBookkeeper(
            "sqlite:///%s" % database_location, database_lock_location)


class TestLMDBBlockStoreBookkeeper(_TestBlockStoreBookkeeper):
    """
    Tests for `LMDBBlockStoreBookkeeper`.
    """
    def setUp(self):
        self._temp_directory_manager = TempManager()
        super(TestLMDBBlockStoreBookkeeper, self).setUp()

    def tearDown(self):
        self._temp_directory_manager.remove_all()

    def test_record_get_with_buffer(self):
        self.bookkeeper.get_record_batch_size = 2
        self.bookkeeper.record_get(LOCATOR_1)
        self.assertEqual(0, len(self.bookkeeper.get_all_get_records()))
        self.bookkeeper.record_get(LOCATOR_2)
        self.assertEqual(2, len(self.bookkeeper.get_all_get_records()))

    def test_record_get_buffer_flushed_on_reference_lost(self):
        directory = self._temp_directory_manager.create_directory()
        bookkeeper = LMDBBlockStoreBookkeeper(
            directory, get_record_batch_size=2)
        bookkeeper.record_get(LOCATOR_1)
        self.assertEqual(0, len(bookkeeper.get_all_get_records()))
        del bookkeeper
        bookkeeper = LMDBBlockStoreBookkeeper(directory)
        records = bookkeeper.get_all_get_records()
        self.assertEqual(1, len(records))
        self.assertEqual(LOCATOR_1, list(records)[0].locator)

    def _create_bookkeeper(self):
        database_location = self._temp_directory_manager.create_directory()
        return LMDBBlockStoreBookkeeper(database_location)


# Work around to stop unittest from trying to run the abstract base class
del _TestBlockStoreBookkeeper
