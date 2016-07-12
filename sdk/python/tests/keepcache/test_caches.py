import shutil
import unittest
from abc import ABCMeta, abstractmethod
from tempfile import mkdtemp
from threading import Lock, Thread, Semaphore

from mock import patch, MagicMock

from arvados.keepcache.block_store import LMDBBlockStore, RecordingBlockStore
from arvados.keepcache.block_store_recorder import \
    DatabaseBlockStoreUsageRecorder
from arvados.keepcache.caches import InMemoryKeepBlockCache, \
    KeepBlockCacheWithBlockStore
from arvados.keepcache.slots import CacheSlot

_LOCATOR_1 = "3b83ef96387f14655fc854ddc3c6bd57"
_LOCATOR_2 = "73f1eb20517c55bf9493b7dd6e480788"
_CACHE_SIZE = 1 * 1024 * 1024
_CONTENTS = bytearray(128)


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
        self.cache = self._create_cache(_CACHE_SIZE)

    def test_get_when_not_set(self):
        self.assertIsNone(self.cache.get("other"))

    def test_get_when_set(self):
        self.cache.reserve_cache("other_1")
        existing_slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        self.cache.reserve_cache("other_2")
        slot = self.cache.get(existing_slot.locator)
        # Checking for identity equality because `CacheSlot`'s blocking `get`
        # method implies only one model of a cache slot for a given locator
        # should exist at a time
        self.assertEqual(id(existing_slot), id(slot))

    def test_reserve_cache_when_not_reserved_before(self):
        slot, just_created = self.cache.reserve_cache(_LOCATOR_1)
        self.assertIsInstance(slot, CacheSlot)
        self.assertEqual(_LOCATOR_1, slot.locator)
        self.assertTrue(just_created)

    def test_reserve_cache_when_reserved_before(self):
        existing_slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot, just_created = self.cache.reserve_cache(existing_slot.locator)
        self.assertEqual(id(slot), id(existing_slot))
        self.assertFalse(just_created)

    def test_cap_cache_when_under_max_cache_size(self):
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(_CONTENTS)
        self.cache.cap_cache()
        # Assert that the `cap_cache` operation has not deleted the slot
        self.assertEqual(slot, self.cache.get(slot.locator))

    def test_put_into_cache_when_full(self):
        content = bytearray(_CACHE_SIZE / 2)
        block_writes = 10
        assert len(content) * block_writes > _CACHE_SIZE
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
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(bytearray(_CACHE_SIZE + 1))
        self.cache.cap_cache()
        self.assertIsNone(self.cache.get(slot.locator))

    def _create_cache(self, cache_size):
        return InMemoryKeepBlockCache(cache_size)


class TestKeepBlockCacheWithLMDB(TestKeepBlockCache):
    """
    Tests for `KeepBlockCacheWithBlockStore`.
    """
    def setUp(self):
        self.working_directory = mkdtemp()
        super(TestKeepBlockCacheWithLMDB, self).setUp()

    def tearDown(self):
        shutil.rmtree(self.working_directory)

    def test_set_content_to_more_than_max_size(self):
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        self.assertRaises(ValueError, slot.set, bytearray(_CACHE_SIZE + 1))

    def test_get_when_stored_in_database_previously(self):
        previous_cache = self._create_cache(_CACHE_SIZE)
        existing_slot, _ = previous_cache.reserve_cache(_LOCATOR_1)
        assert _CACHE_SIZE >= len(_CONTENTS)
        existing_slot.set(_CONTENTS)
        slot = self.cache.get(_LOCATOR_1)
        self.assertEqual(_CONTENTS, slot.get())

    def test_create_cache_slot_when_reference_exists(self):
        slot_1 = self.cache.create_cache_slot(_LOCATOR_1, _CONTENTS)
        slot_2 = self.cache.create_cache_slot(_LOCATOR_1, bytearray(0))
        self.assertEqual(id(slot_1), id(slot_2))

    # FIXME: Use simplier block store!
    # @patch("lmdb.open")
    # def test_concurrent_set_for_same_locator(self, lmdb_open):
    #     puts = []
    #     puts_lock = Lock()
    #     continue_lock = Lock()
    #     continue_lock.acquire()
    #     continued = Semaphore(0)
    #
    #     def pause_put(*args, **kwargs):
    #         # The joys of using a version of Python that was outmoded in 2008...
    #         # http://stackoverflow.com/questions/3190706/nonlocal-keyword-in-python-2-x
    #         with puts_lock:
    #             puts.append(True)
    #             pause = len(puts) == 1
    #         if pause:
    #             continue_lock.acquire()
    #         continued.release()
    #
    #     lmdb_open().begin().__enter__().put = MagicMock(side_effect=pause_put)
    #
    #     cache = self._create_cache(_CACHE_SIZE)
    #     slot_1, _ = cache.reserve_cache(_LOCATOR_1)
    #     slot_2, _ = cache.reserve_cache(_LOCATOR_2)
    #     Thread(target=slot_1.set, args=(bytearray(1),)).start()
    #     Thread(target=slot_1.set, args=(bytearray(2),)).start()
    #     Thread(target=slot_2.set, args=(bytearray(3),)).start()
    #     continue_lock.release()
    #
    #     while len(puts) != 3:
    #         continued.acquire()
    #     # No guarantees on value of slot contents after race condition!
    #     self.assertLessEqual(len(slot_1.content), 2)

    def _create_cache(self, cache_size):
        block_store = RecordingBlockStore(
            # FIXME: a lmdb database of size X bytes cannot hold X bytes of
            # data: a certain amount of the size is not usable. However, it is
            # unclear how much is usable! Getting around this issue for now by
            # making the database slightly larger than the (known) cache size.
            LMDBBlockStore(self.working_directory, cache_size),
            DatabaseBlockStoreUsageRecorder(
                "sqlite:///%s/usage.db" % self.working_directory)
        )
        return KeepBlockCacheWithBlockStore(block_store, cache_size)


# Work around to stop unittest from trying to run the abstract base class
del TestKeepBlockCache
