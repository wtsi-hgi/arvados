import unittest
from UserDict import UserDict
from abc import ABCMeta, abstractmethod
from bisect import bisect_left
from threading import Event, Thread
from time import sleep
from types import MethodType

from arvados.keepcache.block_load_managers import InMemoryBlockLoadManager
from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper, BlockPutRecord, \
    BlockDeleteRecord
from arvados.keepcache.block_store_records import BlockGetRecord, \
    BlockPutRecord, BlockDeleteRecord
from arvados.keepcache.block_stores import LMDBBlockStore, \
    InMemoryBlockStore, BookkeepingBlockStore, LoadCommunicationBlockStore
from tests.keepcache._common import CONTENTS, LOCATOR_2, CACHE_SIZE, \
    LOCATOR_1, TempManager


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


class _TestBlockStore(unittest.TestCase):
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
        self._temp_manager = TempManager()
        self.block_store, self.size = self._create_block_store()

    def tearDown(self):
        self._temp_manager.remove_all()

    def test_exists(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        self.assertTrue(self.block_store.exists(LOCATOR_1))

    def test_exists_when_does_not_exist(self):
        self.assertFalse(self.block_store.exists(LOCATOR_1))

    def test_get(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        self.assertEqual(CONTENTS, self.block_store.get(LOCATOR_1))

    def test_get_when_does_not_exist(self):
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

    def test_put_copy_from_get(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        contents = self.block_store.get(LOCATOR_1)
        assert self.block_store.get(LOCATOR_2) is None
        self.block_store.put(LOCATOR_2, contents)
        self.assertEqual(CONTENTS, self.block_store.get(LOCATOR_1))
        self.assertEqual(CONTENTS, self.block_store.get(LOCATOR_2))

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


class TestInMemoryBlockStore(_TestBlockStore):
    """
    Tests for `InMemoryBlockStore`.
    """
    def _create_block_store(self):
        return InMemoryBlockStore(), CACHE_SIZE


class TestLMDBBlockStore(_TestBlockStore):
    """
    Tests for `LMDBBlockStore`.
    """
    def _create_block_store(self):
        temp_directory = self._temp_manager.create_directory()
        block_store = LMDBBlockStore(temp_directory, CACHE_SIZE)
        return block_store, block_store.calculate_usable_size()


class TestLoadCommunicationBlockStore(_TestBlockStore):
    """
    Tests for `LoadCommunicationBlockStore`.
    """
    def test_get_waits_for_load(self):
        block_load_manager = InMemoryBlockLoadManager(float("inf"))
        block_store, _ = self._create_block_store_with_load_manager(
            block_load_manager)
        block_store.poll_period = 0.01

        event = Event()
        wait_for_get = self._wait_for_get(block_store, event, CONTENTS)

        timestamp = block_load_manager.reserve_load_rights(LOCATOR_1)

        Thread(target=wait_for_get, args=(LOCATOR_1, )).start()
        sleep(0.1)
        self.assertFalse(event.is_set())

        block_store.put(LOCATOR_1, CONTENTS)
        block_load_manager.relinquish_load_rights(LOCATOR_1, timestamp)

        get_complete = event.wait(timeout=10.0)
        self.assertTrue(get_complete)

    def test_get_can_timeout(self):
        block_load_manager = InMemoryBlockLoadManager(0.1)
        block_store, _ = self._create_block_store_with_load_manager(
            block_load_manager)
        block_store.poll_period = 0.01

        block_load_manager.reserve_load_rights(LOCATOR_1)
        self.assertIsNone(block_store.get(LOCATOR_1))

    def test_methods_on_blockstore_but_not_on_interface_work(self):
        block_load_manager = InMemoryBlockLoadManager(0.1)
        underlying_block_store = InMemoryBlockStore()
        underlying_block_store.method_outside_interface = MethodType(
            lambda self: 3, underlying_block_store)
        block_store = LoadCommunicationBlockStore(
            underlying_block_store, block_load_manager)
        self.assertEqual(3, block_store.method_outside_interface())

    def test_get_if_exists_when_does_not_exist(self):
        self.assertIsNone(self.block_store.get_if_exists(LOCATOR_1))

    def test_get_if_exists_when_exists(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        assert self.block_store.exists(LOCATOR_1)
        self.assertEqual(CONTENTS, self.block_store.get_if_exists(LOCATOR_1))

    def _create_block_store(self):
        block_load_manager = InMemoryBlockLoadManager(float("inf"))
        return self._create_block_store_with_load_manager(block_load_manager)

    def _create_block_store_with_load_manager(self, block_load_manager):
        """
        Creates a block store with the given block load manager.
        :param block_load_manager: the block load manager
        :type block_load_manager: BlockLoadManager
        :return: tuple where the first element is the created block store and
        the second is its maximum size in bytes
        :rtype: Tuple[BlockStore, int]
        """
        underlying_block_store = InMemoryBlockStore()
        block_store = LoadCommunicationBlockStore(
            underlying_block_store, block_load_manager)
        return block_store, CACHE_SIZE

    def _wait_for_get(self, block_store, event, assert_get_returns):
        """
        Creates a function that waits for the block with a given locator to be
        loaded from the given block store, before asserting the returned data
        equals that given and then setting the given even.
        :param block_store: the block store
        :type block_store: BlockStore
        :param event: the event to set when the block has loaded
        :type event: Event
        :param assert_get_returns: the expected returned data
        :type assert_get_returns: Any
        :return: the wrapped function
        :rtype: Callable[[str], None]
        """
        def wait_for_get(locator):
            data = block_store.get(locator)
            self.assertEqual(assert_get_returns, data)
            event.set()
        return wait_for_get


class TestBookkeepingBlockStore(_TestBlockStore):
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

    def test_does_not_record_delete_if_not_put(self):
        self.block_store.delete(LOCATOR_1)
        self.assertEqual(0, len(self.bookkeeper.get_all_records()))

    def test_records_delete(self):
        self.block_store.put(LOCATOR_1, CONTENTS)
        put_records = self.bookkeeper.get_all_records()
        self.block_store.delete(LOCATOR_1)
        records = list(self.bookkeeper.get_all_records() - put_records)
        self.assertEqual(1, len(records))
        self.assertIsInstance(records[0], BlockDeleteRecord)
        self.assertEqual(LOCATOR_1, records[0].locator)

    def _create_block_store(self):
        return BookkeepingBlockStore(
            InMemoryBlockStore(),
            InMemoryBlockStoreBookkeeper()
        ), CACHE_SIZE


# Work around to stop unittest from trying to run the abstract base class
del _TestBlockStore
