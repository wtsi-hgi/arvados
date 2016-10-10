import unittest
from abc import ABCMeta, abstractmethod
from threading import Thread, Event

from arvados.keepcache.cache_load_manager import LMDBCacheLoadManager
from tests.keepcache._common import LOCATOR_1, TempManager


class _TestCacheLoadManager(unittest.TestCase):
    """
    Tests for `CacheLoadManager`.
    """
    __metaclass__ = ABCMeta

    @staticmethod
    def _wait_for_load_wrapper(manager, event):
        """
        Wraps a call to `wait_for_load` on the given cache load manager,
        setting the given event when the block locator given to the wrapped
        function is loaded.
        :param manager: the cache load manager
        :type manager: CacheLoadManager
        :param event: the event to set when the block has loaded
        :type event: Event
        :return: the wrapped function
        :rtype: Callable[[str], None]
        """
        def wait_for_load(locator):
            loaded = manager.wait_for_load(locator)
            assert loaded
            event.set()
        return wait_for_load

    @abstractmethod
    def _create_cache_load_manager(self):
        """
        Creates a cache load manager to test with.
        :return: the created cache load manager
        :rtype: CacheLoadManager
        """

    def setUp(self):
        self.cache_load_manager = self._create_cache_load_manager()

    def test_reserve_load_when_not_reserved(self):
        reserved = self.cache_load_manager.reserve_load(LOCATOR_1)
        self.assertTrue(reserved)

    def test_reserve_load_when_already_reserved(self):
        self.cache_load_manager.reserve_load(LOCATOR_1)
        reserved = self.cache_load_manager.reserve_load(LOCATOR_1)
        self.assertFalse(reserved)

    def test_reserve_load_adds_to_pending_loads(self):
        assert LOCATOR_1 not in self.cache_load_manager.pending_loads
        self.cache_load_manager.reserve_load(LOCATOR_1)
        self.assertIn(LOCATOR_1, self.cache_load_manager.pending_loads)

    def test_load_completed(self):
        self.cache_load_manager.reserve_load(LOCATOR_1)
        assert LOCATOR_1 in self.cache_load_manager.pending_loads
        self.cache_load_manager.load_completed(LOCATOR_1)
        self.assertNotIn(LOCATOR_1, self.cache_load_manager.pending_loads)

    def test_wait_for_load(self):
        event = Event()
        wait_for_load = type(self)._wait_for_load_wrapper(
            self.cache_load_manager, event)

        self.cache_load_manager.reserve_load(LOCATOR_1)
        Thread(target=wait_for_load, args=(LOCATOR_1, )).start()
        self.cache_load_manager.load_completed(LOCATOR_1)

        load_detected = event.wait(timeout=10.0)
        self.assertTrue(load_detected)

    def test_wait_for_load_times_out(self):
        self.cache_load_manager.poll_period = 0.05
        self.cache_load_manager.reserve_load(LOCATOR_1)
        loaded = self.cache_load_manager.wait_for_load(LOCATOR_1, timeout=0.25)
        self.assertFalse(loaded)


class TestLMDBCacheLoadManager(_TestCacheLoadManager):
    """
    Tests for `LMDBCacheLoadManager`.
    """
    def setUp(self):
        self._temp_manager = TempManager()
        super(TestLMDBCacheLoadManager, self).setUp()

    def tearDown(self):
        self._temp_manager.remove_all()
        super(TestLMDBCacheLoadManager, self).tearDown()

    def _create_cache_load_manager(self):
        directory = self._temp_manager.create_directory()
        return LMDBCacheLoadManager(directory)


# Stop unittest from trying to instantiate the abstract base class as a test
del _TestCacheLoadManager


if __name__ == "__main__":
    unittest.main()
