import threading
import unittest

from mock import MagicMock

from arvados.keepcache.slots import CacheSlot, GetterSetterCacheSlot
from tests.keepcache._common import LOCATOR_1, CONTENTS


class TestCacheSlot(unittest.TestCase):
    """
    Unit tests for `CacheSlot`.
    """
    def setUp(self):
        self.cache_slot = CacheSlot(LOCATOR_1)

    def test_content_when_not_set(self):
        self.assertIsNone(self.cache_slot.content)

    def test_set_and_get(self):
        self.cache_slot.set(CONTENTS)
        self.assertEqual(CONTENTS, self.cache_slot.get())
        self.assertEqual(CONTENTS, self.cache_slot.content)

    def test_size_when_not_set(self):
        self.assertEqual(0, self.cache_slot.size())

    def test_size_when_set(self):
        self.cache_slot.set(CONTENTS)
        self.assertEqual(len(CONTENTS), self.cache_slot.size())


class TestGetterSetterCacheSlot(TestCacheSlot):
    """
    Unit tests for `GetterSetterCacheSlot`.
    """
    def setUp(self):
        super(TestGetterSetterCacheSlot, self).setUp()
        self.getter = MagicMock()
        self.setter = MagicMock()
        self.cache_slot = GetterSetterCacheSlot(
            LOCATOR_1, self.getter, self.setter)

    def test_get_when_in_external(self):
        self.getter.return_value = CONTENTS
        self.assertEqual(CONTENTS, self.cache_slot.get())

    def test_set_stores_externally(self):
        self.cache_slot.set(CONTENTS)
        self.setter.assert_called_once_with(self.cache_slot.locator, CONTENTS)

