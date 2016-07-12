import shutil
import unittest
from abc import ABCMeta, abstractmethod
from tempfile import mkdtemp

from arvados.keepcache.block_store import LMDBBlockStore, InMemoryBlockStore
from tests.keepcache._common import CONTENTS, CACHE_SIZE
from tests.keepcache._common import LOCATOR_1


class TestBlockStore(unittest.TestCase):
    """
    Tests for `BlockStore`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_block_store(self):
        """
        Creates a block store that is to be tested.
        :return: the created block store
        :rtype: BlockStore
        """

    def setUp(self):
        self.block_store = self._create_block_store()

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
        contents = bytearray(CACHE_SIZE)
        self.block_store.put(LOCATOR_1, contents)
        self.assertEqual(contents, self.block_store.get(LOCATOR_1))

    def test_delete(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        assert self.block_store.get(LOCATOR_1) == CONTENTS
        self.block_store.delete(LOCATOR_1)
        self.assertIsNone(self.block_store.get(LOCATOR_1))

    def test_delete_frees_space(self):
        for _ in range(5):
            self.block_store.put(LOCATOR_1, bytearray(CACHE_SIZE / 2))
            self.block_store.delete(LOCATOR_1)


class TestInMemoryBlockStore(TestBlockStore):
    """
    Tests for `InMemoryBlockStore`.
    """
    def _create_block_store(self):
        return InMemoryBlockStore()


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
        return LMDBBlockStore(temp_directory, CACHE_SIZE)


# TODO
# class TestRecordingBlockStore(TestBlockStore):
#     """
#     Tests for `RecordingBlockStore`.
#     """
#     def _create_block_store(self):
#         return RecordingBlockStore()


# Work around to stop unittest from trying to run the abstract base class
del TestBlockStore
