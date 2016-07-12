import os
import tempfile
import unittest
from abc import ABCMeta

from arvados.keepcache.block_store_recorder import DatabaseBlockStoreUsageRecorder, \
    InMemoryBlockStoreUsageRecorder
from arvados.keepcache.replacement_policies import FIFOCacheReplacementPolicy
from tests.keepcache._common import LOCATOR_1, LOCATOR_2

_CONTENT_SIZE = 10


class CacheReplacementPolicy(unittest.TestCase):
    """
    Tests for `CacheReplacementPolicy`.
    """
    __metaclass__ = ABCMeta

    def setUp(self):
        self.recorder = InMemoryBlockStoreUsageRecorder()


class TestFIFOCacheReplacementPolicy(CacheReplacementPolicy):
    """
    Tests for `FIFOCacheReplacementPolicy`.
    """
    def setUp(self):
        super(TestFIFOCacheReplacementPolicy, self).setUp()
        self.policy = FIFOCacheReplacementPolicy()

    def test_next_to_delete_when_none(self):
        to_delete = self.policy.next_to_delete(self.recorder)
        self.assertIsNone(to_delete)

    def test_next_to_delete_when_older_block(self):
        self.recorder.record_put(LOCATOR_1, _CONTENT_SIZE)
        self.recorder.record_put(LOCATOR_2, _CONTENT_SIZE)
        self.assertEqual(LOCATOR_1, self.policy.next_to_delete(self.recorder))


# Work around to stop unittest from trying to run the abstract base class
del CacheReplacementPolicy
