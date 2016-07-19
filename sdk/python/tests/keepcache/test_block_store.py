import shutil
import unittest
from UserDict import UserDict
from abc import ABCMeta, abstractmethod
from bisect import bisect_left
from tempfile import mkdtemp

from arvados.keepcache.block_store import LMDBBlockStore, InMemoryBlockStore, \
    BookkeepingBlockStore, RocksDBBlockStore, DiskOnlyBlockStore
from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper, BlockGetRecord, BlockPutRecord, \
    BlockDeleteRecord
from tests.keepcache._common import CONTENTS, CACHE_SIZE, LOCATOR_1


class _LazyCalculatedDict(UserDict):
    """
    Dict with values that are lazily calculated using a calculator function.
    """
    def __init__(self, calculator, *args, **kwargs):
        UserDict.__init__(self, *args, **kwargs)
        self._calculator = calculator

    def __getitem__(self, key):
        if key not in self.data:
            self.data[key] = self._calculator(key)
        return self.data[key]

    def __setitem__(self, key, value):
        if self._calculator(key) != value:
            raise ValueError("")
        self.data[key] = value


class TestBlockStore(unittest.TestCase):
    """
    Tests for `BlockStore`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_block_store(self):
        """
        Creates a block store that is to be tested.
        :return: a tuple where the first item is the created block store and the
        second is the total capacity of the block store in bytes (capacity
        should be around `CACHE_SIZE`)
        :rtype: Tuple[BlockStore, int]
        """

    def setUp(self):
        self._temp_directories = []
        self.block_store, self.size = self._create_block_store()

    def tearDown(self):
        for directory in self._temp_directories:
            shutil.rmtree(directory)

    def test_put_and_get(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        self.assertEqual(CONTENTS, self.block_store.get(LOCATOR_1))

    def test_get_when_not_put(self):
        self.assertIsNone(self.block_store.get(LOCATOR_1))

    def test_put_when_exists(self):
        assert len(CONTENTS) >= 1
        self.block_store.put(LOCATOR_1, bytearray(len(CONTENTS) - 1))
        self.block_store.put(LOCATOR_1, CONTENTS)
        self.assertEqual(CONTENTS, self.block_store.get(LOCATOR_1))

    def test_put_maximum_amount(self):
        def size_to_actual_size_mapper(size):
            contents = bytearray(size)
            return self.block_store.calculate_stored_size(contents)

        sizes = _LazyCalculatedDict(size_to_actual_size_mapper)
        # `bisect_left` used to find the size value that gets as close to the
        # actual maximum size as possible without exceeding it
        actual_size = bisect_left(sizes, self.size, hi=self.size)
        assert self.size >= actual_size > 0
        contents = bytearray(actual_size)
        self.block_store.put(LOCATOR_1, contents)
        self.assertEqual(contents, self.block_store.get(LOCATOR_1))

    def test_delete_when_not_put(self):
        deleted = self.block_store.delete(LOCATOR_1)
        self.assertFalse(deleted)

    def test_delete_when_put(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        assert self.block_store.get(LOCATOR_1) == CONTENTS
        deleted = self.block_store.delete(LOCATOR_1)
        self.assertTrue(deleted)
        self.assertIsNone(self.block_store.get(LOCATOR_1))

    def test_delete_frees_space(self):
        proportion_capacity = 0.3
        size = int(CACHE_SIZE * proportion_capacity)
        assert size > 0
        contents = bytearray(size)
        for _ in range(int(1 / proportion_capacity) * 10):
            assert self.block_store.get(LOCATOR_1) is None
            self.block_store.put(LOCATOR_1, contents)
            deleted = self.block_store.delete(LOCATOR_1)
            assert deleted
            self.assertIsNone(self.block_store.get(LOCATOR_1))

    def _create_temp_directory(self):
        """
        Creates a temporary directory that is deleted upon tear down.
        :return: the temporary directory
        :rtype: str
        """
        temp_directory = mkdtemp()
        self._temp_directories.append(temp_directory)
        return temp_directory


class TestInMemoryBlockStore(TestBlockStore):
    """
    Tests for `InMemoryBlockStore`.
    """
    def _create_block_store(self):
        return InMemoryBlockStore(), CACHE_SIZE


class TestDiskOnlyBlockStore(TestBlockStore):
    """
    Tests for `DiskOnlyBlockStore`.
    """

    def _create_block_store(self):
        temp_directory = self._create_temp_directory()
        block_store = DiskOnlyBlockStore(temp_directory)
        return block_store, CACHE_SIZE


class TestLMDBBlockStore(TestBlockStore):
    """
    Tests for `LMDBBlockStore`.
    """
    def _create_block_store(self):
        temp_directory = self._create_temp_directory()
        block_store = LMDBBlockStore(temp_directory, CACHE_SIZE)
        return block_store, block_store.calculate_usuable_size()


class TestRocksDBBlockStore(TestBlockStore):
    """
    Tests for `RocksDBBlockStore`.
    """
    def _create_block_store(self):
        temp_directory = self._create_temp_directory()
        return RocksDBBlockStore(temp_directory), CACHE_SIZE


class TestBookkeepingBlockStore(TestBlockStore):
    """
    Tests for `BookkeepingBlockStore`.
    """
    def setUp(self):
        super(TestBookkeepingBlockStore, self).setUp()
        self.bookkeeper = self.block_store.bookkeeper

    def test_records_get(self):
        self.block_store.get(LOCATOR_1)
        records = list(self.bookkeeper.get_all_records())
        self.assertEqual(1, len(records))
        self.assertIsInstance(records[0], BlockGetRecord)
        self.assertEqual(LOCATOR_1, records[0].locator)

    def test_records_put(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        records = list(self.bookkeeper.get_all_records())
        self.assertEqual(1, len(records))
        self.assertIsInstance(records[0], BlockPutRecord)
        self.assertEqual(LOCATOR_1, records[0].locator)
        self.assertEqual(
            self.block_store.calculate_stored_size(CONTENTS), records[0].size)

    def test_records_delete(self):
        self.block_store.delete(LOCATOR_1)
        records = list(self.bookkeeper.get_all_records())
        self.assertEqual(1, len(records))
        self.assertIsInstance(records[0], BlockDeleteRecord)
        self.assertEqual(LOCATOR_1, records[0].locator)

    def _create_block_store(self):
        return BookkeepingBlockStore(
            InMemoryBlockStore(),
            InMemoryBlockStoreBookkeeper()
        ), CACHE_SIZE


# Work around to stop unittest from trying to run the abstract base class
del TestBlockStore
