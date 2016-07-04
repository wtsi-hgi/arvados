import shutil
import unittest
from abc import ABCMeta, abstractmethod
from tempfile import mkdtemp

from arvados.keep_cache import BasicKeepBlockCache, \
    KeepBlockCacheWithLMDB, CacheSlot

_LOCATOR_1 = "3b83ef96387f14655fc854ddc3c6bd57"
_LOCATOR_2 = "73f1eb20517c55bf9493b7dd6e480788"
_CACHE_SIZE = 1 * 1024 * 1024
_CONTENTS = bytearray(128)


class TestCacheSlot(unittest.TestCase):
    """
    Unit tests for `CacheSlot`.
    """
    def setUp(self):
        self.cache_slot = CacheSlot(_LOCATOR_1)

    def test_cache_slot_get_when_set(self):
        self.cache_slot.set(_CONTENTS)
        self.assertEqual(_CONTENTS, self.cache_slot.get())

    def test_cache_slot_size_when_not_set(self):
        self.assertEqual(0, self.cache_slot.size())

    def test_cache_slot_size_when_set(self):
        self.cache_slot.set(_CONTENTS)
        self.assertEqual(len(_CONTENTS), self.cache_slot.size())


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
        existing_slot, _ = self.cache.reserve_cache(_LOCATOR_1)
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


class TestBasicKeepBlockCache(TestKeepBlockCache):
    """
    Tests for `BasicKeepBlockCache`.
    """
    def test_cap_cache_when_over_max_cache_size(self):
        # This test is not universal for all `KeepBlockCache` implementations
        # as not all (strangly) allow the cache to exceed its max size
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        slot.set(bytearray(_CACHE_SIZE + 1))
        self.cache.cap_cache()
        self.assertIsNone(self.cache.get(slot.locator))

    def _create_cache(self, cache_size):
        return BasicKeepBlockCache(cache_size)


class TestKeepBlockCacheWithLMDB(TestKeepBlockCache):
    """
    Tests for `KeepBlockCacheWithLMDB`.
    """
    def setUp(self):
        self.database_directory = mkdtemp()
        super(TestKeepBlockCacheWithLMDB, self).setUp()

    def tearDown(self):
        shutil.rmtree(self.database_directory)

    def test_cap_cache_when_over_max_cache_size(self):
        slot, _ = self.cache.reserve_cache(_LOCATOR_1)
        self.assertRaises(ValueError, slot.set, bytearray(_CACHE_SIZE + 1))

    def test_get_when_stored_in_database_previously(self):
        previous_cache = self._create_cache(_CACHE_SIZE)
        existing_slot, _ = previous_cache.reserve_cache(_LOCATOR_1)
        assert _CACHE_SIZE >= len(_CONTENTS)
        existing_slot.set(_CONTENTS)
        slot = self.cache.get(_LOCATOR_1)
        self.assertEqual(_CONTENTS, slot.get())

    def _create_cache(self, cache_size):
        return KeepBlockCacheWithLMDB(cache_size, self.database_directory)


# Work around to stop unittest from trying to run the abstract base class
del TestKeepBlockCache
