import unittest

from arvados.keepcache.slots import CacheSlot
from tests.keepcache.test_caches import _LOCATOR_1, _CONTENTS


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
