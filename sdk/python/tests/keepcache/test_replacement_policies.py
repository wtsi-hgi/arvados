import unittest
from abc import ABCMeta, abstractmethod

from arvados.keepcache.block_store_bookkeepers import \
    InMemoryBlockStoreBookkeeper
from arvados.keepcache.replacement_policies import FIFOCacheReplacementPolicy, \
    LastUsedReplacementPolicy, CacheReplacementPolicy
from tests.keepcache._common import LOCATOR_1, LOCATOR_2

_CONTENT_SIZE = 10


class _TestCacheReplacementPolicy(unittest.TestCase):
    """
    Tests for `CacheReplacementPolicy`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def _create_policy(self):
        """
        Creates an instance of the cache replacement policy that is being
        tested.
        :return: the created policy
        :rtype: CacheReplacementPolicy
        """

    def setUp(self):
        self.recorder = InMemoryBlockStoreBookkeeper()
        self.policy = self._create_policy()

    def test_next_to_delete_when_none(self):
        to_delete = self.policy.next_to_delete(self.recorder)
        self.assertIsNone(to_delete)


class TestFIFOCacheReplacementPolicy(_TestCacheReplacementPolicy):
    """
    Tests for `FIFOCacheReplacementPolicy`.
    """
    def test_next_to_delete_when_older_block(self):
        self.recorder.record_put(LOCATOR_1, _CONTENT_SIZE)
        self.recorder.record_put(LOCATOR_2, _CONTENT_SIZE)
        self.assertEqual(LOCATOR_1, self.policy.next_to_delete(self.recorder))

    def _create_policy(self):
        return FIFOCacheReplacementPolicy()


class TestLastUsedReplacementPolicy(_TestCacheReplacementPolicy):
    """
    Tests for `LastUsedReplacementPolicy`.
    """
    def test_next_to_delete_when_only_put(self):
        self.recorder.record_put(LOCATOR_1, _CONTENT_SIZE)
        self.recorder.record_put(LOCATOR_2, _CONTENT_SIZE)
        self.assertEqual(LOCATOR_1, self.policy.next_to_delete(self.recorder))

    def test_next_to_delete_when_puts_and_gets(self):
        self.recorder.record_get(LOCATOR_2)
        self.recorder.record_put(LOCATOR_1, _CONTENT_SIZE)
        self.recorder.record_put(LOCATOR_2, _CONTENT_SIZE)
        self.recorder.record_get(LOCATOR_1)
        self.assertEqual(LOCATOR_2, self.policy.next_to_delete(self.recorder))

    def test_next_to_delete_when_puts_gets_and_deletes(self):
        self.recorder.record_put(LOCATOR_1, _CONTENT_SIZE)
        self.recorder.record_get(LOCATOR_1)
        self.recorder.record_put(LOCATOR_2, _CONTENT_SIZE)
        self.recorder.record_delete(LOCATOR_1)
        self.recorder.record_get(LOCATOR_1)
        self.recorder.record_put(LOCATOR_1, _CONTENT_SIZE)
        self.recorder.record_get(LOCATOR_2)
        self.assertEqual(LOCATOR_1, self.policy.next_to_delete(self.recorder))

    def _create_policy(self):
        return LastUsedReplacementPolicy()


# Work around to stop unittest from trying to run the abstract base class
del _TestCacheReplacementPolicy
