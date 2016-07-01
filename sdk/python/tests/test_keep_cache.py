import unittest
from abc import ABCMeta


class TestKeepBlockCache(unittest.TestCase):
    """
    Unit tests for `KeepBlockCache`.
    """
    __metaclass__ = ABCMeta

    def test_reserve_cache_when_not_reserved_before(self):

