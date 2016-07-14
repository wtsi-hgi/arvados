import shutil
import unittest
from UserDict import UserDict
from abc import ABCMeta, abstractmethod
from bisect import bisect_left
from tempfile import mkdtemp

from arvados.keepcache.block_store import LMDBBlockStore, InMemoryBlockStore
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
        self.block_store, self.size = self._create_block_store()

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
        write_size = 1
        while True:
            contents = bytearray(write_size)
            size_in_store = self.block_store.calculate_stored_size(contents)

            if size_in_store <= CACHE_SIZE:
                self.block_store.put(LOCATOR_1, contents)
                self.block_store.delete(LOCATOR_1)
                write_size *= 2
            else:
                break


class TestInMemoryBlockStore(TestBlockStore):
    """
    Tests for `InMemoryBlockStore`.
    """
    def _create_block_store(self):
        return InMemoryBlockStore(), CACHE_SIZE


class TestLMDBBlockStore(TestBlockStore):
    """
    Tests for `LMDBBlockStore`.
    """
    def setUp(self):
        self._temp_directories = []
        super(TestLMDBBlockStore, self).setUp()

    def tearDown(self):
        for directory in self._temp_directories:
            shutil.rmtree(directory)

    def _create_block_store(self):
        temp_directory = mkdtemp()
        self._temp_directories.append(temp_directory)
        block_store = LMDBBlockStore(temp_directory, CACHE_SIZE)
        return block_store, block_store.calculate_usuable_size()


# TODO
# class TestRecordingBlockStore(TestBlockStore):
#     """
#     Tests for `RecordingBlockStore`.
#     """
#     def _create_block_store(self):
#         return RecordingBlockStore()


# Work around to stop unittest from trying to run the abstract base class
del TestBlockStore
