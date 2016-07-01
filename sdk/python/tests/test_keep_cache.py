import unittest
from abc import ABCMeta, abstractmethod

from arvados.keep import KeepBlockCache, BasicKeepBlockCache, \
    KeepBlockCacheWithLMDB

_LOCATOR_1 = "3b83ef96387f14655fc854ddc3c6bd57"
_LOCATOR_2 = "73f1eb20517c55bf9493b7dd6e480788"
_CACHE_SIZE = 8


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

    def test_cache_slot_get_when_not_set(self):
        self.assertIsNone(self.CacheSlot().get())

    def test_cache_slot_get_when_set(self):
        cache_slot = self.CacheSlot(_LOCATOR_1)
        contents = bytes(8)
        cache_slot.set(contents)
        self.assertEqual(contents, cache_slot.get())

    def test_cache_slot_size_when_not_set(self):
        cache_slot = self.CacheSlot(_LOCATOR_1)
        self.assertEqual(0, cache_slot.size())

    def test_cache_slot_size_when_set(self):
        cache_slot = self.CacheSlot(_LOCATOR_1)
        size = 256
        cache_slot.set(bytes(size))
        self.assertEqual(size, cache_slot.size())

    def test_get_when_not_set(self):
        self.assertIsNone(self.cache.get())

    def test_get_when_set(self):
        slot = self.cache.reserve_cache(_LOCATOR_1)
        self.assertEqual(self.cache.get(slot.locator), slot)

    def test_reserve_cache_when_not_reserved_before(self):
        slot = self.cache.reserve_cache(_LOCATOR_1)
        self.assertIsInstance(slot, KeepBlockCache.CacheSlot)
        self.assertEqual(_LOCATOR_1, slot.locator)

    def test_reserve_cache_when_reserved_before(self):
        existing_slot = self.cache.reserve_cache(_LOCATOR_1)
        self.assertEqual(
            self.cache.reserve_cache(existing_slot.locator), existing_slot)

    def test_cap_cache_when_under_max_cache_size(self):
        slot = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(bytes(_CACHE_SIZE))
        self.cache.cap_cache()
        self.assertEqual(slot, self.cache.get(slot.locator))

    def test_cap_cache_when_over_max_cache_size(self):
        slot = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(bytes(_CACHE_SIZE + 1))
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
