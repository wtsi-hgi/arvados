import unittest
from abc import ABCMeta, abstractmethod

from multiprocessing import Lock, Semaphore
from threading import Thread

from mock import MagicMock

from arvados.keepcache.block_store import BookkeepingBlockStore, \
    InMemoryBlockStore
from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper
from arvados.keepcache.caches import InMemoryKeepBlockCache, \
    KeepBlockCacheWithBlockStore
from arvados.keepcache.slots import CacheSlot
from tests.keepcache._common import LOCATOR_1, CACHE_SIZE, CONTENTS, LOCATOR_2


class TestKeepBlockCache(unittest.TestCase):
    """
    Unit tests for `KeepBlockCache`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_cache(self, cache_size):
        """
        Creates the cache that is to be tested.
        :param cache_size: the size of the cache that should be created in bytes
        :type cache_size: int
        :return: the cache that is to be tested
        :rtype KeepBlockCache
        """

    def setUp(self):
        self.cache = self._create_cache(CACHE_SIZE)

    def test_get_when_not_set(self):
        self.assertIsNone(self.cache.get("other"))

    def test_get_when_set(self):
        self.cache.reserve_cache("other_1")
        existing_slot, _ = self.cache.reserve_cache(LOCATOR_1)
        self.cache.reserve_cache("other_2")
        slot = self.cache.get(existing_slot.locator)
        # Checking for identity equality because `CacheSlot`'s blocking `get`
        # method implies only one model of a cache slot for a given locator
        # should exist at a time
        self.assertEqual(id(existing_slot), id(slot))

    def test_reserve_cache_when_not_reserved_before(self):
        slot, just_created = self.cache.reserve_cache(LOCATOR_1)
        self.assertIsInstance(slot, CacheSlot)
        self.assertEqual(LOCATOR_1, slot.locator)
        self.assertTrue(just_created)

    def test_reserve_cache_when_reserved_before(self):
        existing_slot, _ = self.cache.reserve_cache(LOCATOR_1)
        slot, just_created = self.cache.reserve_cache(existing_slot.locator)
        self.assertEqual(id(slot), id(existing_slot))
        self.assertFalse(just_created)

    def test_cap_cache_when_under_max_cache_size(self):
        slot, _ = self.cache.reserve_cache(LOCATOR_1)
        slot.set(CONTENTS)
        self.cache.cap_cache()
        # Assert that the `cap_cache` operation has not deleted the slot
        self.assertEqual(slot, self.cache.get(slot.locator))

    def test_put_into_cache_when_full(self):
        content = bytearray(CACHE_SIZE / 2)
        block_writes = 10
        assert len(content) * block_writes > CACHE_SIZE
        for i in range(block_writes):
            locator = "block_%s" % i
            slot, _ = self.cache.reserve_cache(locator)
            slot.set(content)
        # We can't reason at all about what might be left in the cache as that
        # depends on the caching policy used


class TestInMemoryKeepBlockCache(TestKeepBlockCache):
    """
    Tests for `InMemoryKeepBlockCache`.
    """
    def test_cap_cache_when_over_max_cache_size(self):
        # This test is not universal for all `KeepBlockCache` implementations
        # as not all (strangly) allow the cache to exceed its max size
        slot, _ = self.cache.reserve_cache(LOCATOR_1)
        slot.set(bytearray(CACHE_SIZE + 1))
        self.cache.cap_cache()
        self.assertIsNone(self.cache.get(slot.locator))

    def _create_cache(self, cache_size):
        return InMemoryKeepBlockCache(cache_size)


class TestKeepBlockCacheWithBlockStore(TestKeepBlockCache):
    """
    Tests for `KeepBlockCacheWithBlockStore`.
    """
    def test_get_when_set_in_block_store_in_previous_cache(self):
        self.cache.block_store.put(LOCATOR_1, CONTENTS)
        self.assertEqual(CONTENTS, self.cache.get(LOCATOR_1).content)


    def test_set_content_to_more_than_max_size(self):
        slot, _ = self.cache.reserve_cache(LOCATOR_1)
        self.assertRaises(ValueError, slot.set, bytearray(CACHE_SIZE + 1))

    def test_create_cache_slot_when_reference_exists(self):
        slot_1 = self.cache.create_cache_slot(LOCATOR_1, CONTENTS)
        slot_2 = self.cache.create_cache_slot(LOCATOR_1, bytearray(0))
        self.assertEqual(id(slot_1), id(slot_2))

    def test_concurrent_set_for_same_locator(self):
        puts = []
        puts_lock = Lock()
        continue_lock = Lock()
        continue_lock.acquire()
        continued = Semaphore(0)
        original_put = self.cache.block_store.put

        def pause_put(*args, **kwargs):
            # The joys of using a version of Python that was outmoded in 2008...
            # http://stackoverflow.com/questions/3190706/nonlocal-keyword-in-python-2-x
            with puts_lock:
                puts.append(True)
                pause = len(puts) == 1
            if pause:
                continue_lock.acquire()
            original_put(*args, **kwargs)
            continued.release()

        self.cache.block_store.put = MagicMock(side_effect=pause_put)

        slot_1, _ = self.cache.reserve_cache(LOCATOR_1)
        slot_2, _ = self.cache.reserve_cache(LOCATOR_2)
        Thread(target=slot_1.set, args=(bytearray(1),)).start()
        Thread(target=slot_1.set, args=(bytearray(2),)).start()
        Thread(target=slot_2.set, args=(bytearray(3),)).start()
        continue_lock.release()

        while len(puts) != 3:
            continued.acquire()
        self.assertEqual(bytearray(3), slot_2.content)
        # No guarantees on value of slot contents after race condition!
        self.assertLessEqual(len(slot_1.content), 2)

    def _create_cache(self, cache_size):
        block_store = BookkeepingBlockStore(
            InMemoryBlockStore(),
            InMemoryBlockStoreBookkeeper()
        )
        return KeepBlockCacheWithBlockStore(block_store, cache_size)


# Work around to stop unittest from trying to run the abstract base class
del TestKeepBlockCache
