import unittest
from abc import ABCMeta, abstractmethod
from threading import Thread, Semaphore

from arvados.keep import KeepBlockCache, BasicKeepBlockCache, \
    KeepBlockCacheWithLMDB

_LOCATOR_1 = "3b83ef96387f14655fc854ddc3c6bd57"
_LOCATOR_2 = "73f1eb20517c55bf9493b7dd6e480788"
_CACHE_SIZE = 8
_CONTENTS = bytearray(32)


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
        self.CacheSlot = type(self.cache).CacheSlot

    def test_cache_slot_get_when_set(self):
        cache_slot = self.CacheSlot(_LOCATOR_1)
        cache_slot.set(_CONTENTS)
        self.assertEqual(_CONTENTS, cache_slot.get())

    def test_cache_slot_size_when_not_set(self):
        cache_slot = self.CacheSlot(_LOCATOR_1)
        self.assertEqual(0, cache_slot.size())

    def test_cache_slot_size_when_set(self):
        cache_slot = self.CacheSlot(_LOCATOR_1)
        cache_slot.set(_CONTENTS)
        self.assertEqual(len(_CONTENTS), cache_slot.size())

    def test_get_when_not_set(self):
        self.assertIsNone(self.cache.get("other"))

    def test_get_when_set(self):
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        self.assertEqual(self.cache.get(slot.locator), slot)

    def test_reserve_cache_when_not_reserved_before(self):
        slot, just_created = self.cache.reserve_cache(_LOCATOR_1)
        self.assertIsInstance(slot, KeepBlockCache.CacheSlot)
        self.assertEqual(_LOCATOR_1, slot.locator)
        self.assertTrue(just_created)

    def test_reserve_cache_when_reserved_before(self):
        existing_slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot, just_created = self.cache.reserve_cache(existing_slot.locator)
        self.assertEqual(slot, existing_slot)
        self.assertFalse(just_created)

    def test_cap_cache_when_under_max_cache_size(self):
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(bytearray(_CACHE_SIZE))
        self.cache.cap_cache()
        self.assertEqual(slot, self.cache.get(slot.locator))

    def test_cap_cache_when_over_max_cache_size(self):
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(bytearray(_CACHE_SIZE + 1))
        self.cache.cap_cache()
        self.assertIsNone(self.cache.get(slot.locator))


class TestBasicKeepBlockCache(TestKeepBlockCache):
    """
    Tests for `BasicKeepBlockCache`.
    """
    def _create_cache(self, cache_size):
        return BasicKeepBlockCache(cache_size)


class TestKeepBlockCacheWithLMDB(TestKeepBlockCache):
    """
    Tests for `KeepBlockCacheWithLMDB`.
    """
    def _create_cache(self, cache_size):
        return KeepBlockCacheWithLMDB(cache_size)


# Work around to stop unittest from trying to run the abstract base class
del TestKeepBlockCache
