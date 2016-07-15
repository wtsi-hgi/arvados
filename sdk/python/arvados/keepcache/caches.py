import threading
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from weakref import WeakValueDictionary

from arvados.keepcache.replacement_policies import FIFOCacheReplacementPolicy
from arvados.keepcache.slots import CacheSlot, GetterSetterCacheSlot


class KeepBlockCache(object):
    """
    Base class for Keep block caches.

    This interface was extracted extracted from
    """
    __metaclass__ = ABCMeta

    def __init__(self, cache_max):
        """
        Constructor.
        :param cache_max: the (ideal - the implementation this interface was
        extracted from implies that it is not enforced) maximum amount of memory
        (in bytes) to use for the cache
        :rtype cache_max: int
        """
        self._cache_max = cache_max

    @property
    def cache_max(self):
        """
        Gets the max cache size.
        :return: max cache size in bytes
        :rtype: int
        """
        return self._cache_max

    @abstractmethod
    def get(self, locator):
        """
        Gets the cache slot with the given locator.
        :param locator: the identifier of the entry in this cache
        :type locator: str
        :return: the cache if found, else `None`
        :rtype: Optional[CacheSlot]
        """

    @abstractmethod
    def reserve_cache(self, locator):
        """
        Reserves a cache slot for the given locator or returns the existing slot
        associated with the locator.

        Despite the name, this method does not reserve actual memory in the
        cache.
        :param locator: the identifier of the entry in this cache
        :type locator: str
        :return: a tuple containing the reserved cache slot as the first element
        and whether the slot was newly created as the second element
        :rtype: Tuple[CacheSlot, bool]
        """

    @abstractmethod
    def cap_cache(self):
        """
        Caps the cache size to `self.cache_max` by removing entries from the
        cache if necessary.

        The implementation that this interface was extracted from does not
        enforce that the size of the cache remains less than `self.cache_max`.
        Instead, this method exists to allow external code to force this cache
        back down to the size which, confusingly, was implied by the signature
        of the constructor as the maximum.
        """


class InMemoryKeepBlockCache(KeepBlockCache):
    def __init__(self, cache_max=(256 * 1024 * 1024)):
        super(InMemoryKeepBlockCache, self).__init__(cache_max)
        self.cache_size = cache_max
        self._cache = []
        self._cache_lock = threading.Lock()

    def cap_cache(self):
        '''Cap the cache size to self.cache_max'''
        with self._cache_lock:
            # Select all slots except those where ready.is_set() and content is
            # None (that means there was an error reading the block).
            self._cache = [c for c in self._cache if not (c.ready.is_set() and c.content is None)]
            sm = sum([slot.size() for slot in self._cache])
            while len(self._cache) > 0 and sm > self.cache_size:
                for i in xrange(len(self._cache)-1, -1, -1):
                    if self._cache[i].ready.is_set():
                        del self._cache[i]
                        break
                sm = sum([slot.size() for slot in self._cache])

    def _get(self, locator):
        # Test if the locator is already in the cache
        for i in xrange(0, len(self._cache)):
            if self._cache[i].locator == locator:
                n = self._cache[i]
                if i != 0:
                    # move it to the front
                    del self._cache[i]
                    self._cache.insert(0, n)
                return n
        return None

    def get(self, locator):
        with self._cache_lock:
            return self._get(locator)

    def reserve_cache(self, locator):
        '''Reserve a cache slot for the specified locator,
        or return the existing slot.'''
        with self._cache_lock:
            n = self._get(locator)
            if n:
                return n, False
            else:
                # Add a new cache slot for the locator
                n = CacheSlot(locator)
                self._cache.insert(0, n)
                return n, True


class KeepBlockCacheWithBlockStore(KeepBlockCache):
    """
    Keep block cache backed by a block store, which can hold blocks in any way.
    """
    def __init__(self, block_store, cache_max=20 * 1024 * 1024 * 1024,
                 cache_replacement_policy=FIFOCacheReplacementPolicy()):
        """
        Constructor.
        :param block_store: store for blocks
        :type block_store: BookkeepingBlockStore
        :param cache_max: maximum cache size (default: 20GB)
        :type cache_max: int
        :param cache_replacement_policy: TODO
        :type cache_replacement_policy: CacheReplacementPolicy
        """
        super(KeepBlockCacheWithBlockStore, self).__init__(cache_max)
        # TODO: Make block store read-only
        self.block_store = block_store
        self._cache_replacement_policy = cache_replacement_policy
        self._referenced_cache_slots = WeakValueDictionary()
        self._referenced_cache_slots_lock = threading.Lock()
        self._writing = set()
        self._writing_lock = threading.Lock()
        self._writing_complete_listeners = defaultdict(set)
        self._reserved_space = 0
        self._space_increase_lock = threading.Lock()

    def get(self, locator):
        # Return pre-existing model of slot if referenced elsewhere. This is
        # required to make the cache slot's blocking `get` method work in the
        # same was as it does with the in-memory cache. It also avoids wasting
        # memory with multiple models of the same slot.
        slot = self._referenced_cache_slots.get(locator, None)
        if slot is not None:
            return slot
        else:
            content = self.block_store.get(locator)
            if content is None:
                return None
            else:
                return self.create_cache_slot(locator, content)

    def reserve_cache(self, locator):
        slot = self.get(locator)
        if slot is not None:
            return slot, False
        else:
            return self.create_cache_slot(locator), True

    def cap_cache(self):
        # Given that the cache in this implementation does not grow beyond its
        # allocated size, this operation to trim the cache back down to size
        # should be a noop.
        assert self.block_store.bookkeeper.get_size() <= self.cache_max

    def create_cache_slot(self, locator, content=None):
        """
        Creates a cache slot for the given locator and sets its contents if
        given.

        If a reference already exists to a model of the required cache slot,
        that pre-existing slot is returned.
        :param locator: the slot identifier
        :type locator: str
        :param content: optional contents that the cache slot should hold
        :type content: Optional[bytearray]
        :return: the cache slot
        """
        self._referenced_cache_slots_lock.acquire()
        slot = self._referenced_cache_slots.get(locator, None)
        if slot is not None:
            self._referenced_cache_slots_lock.release()
            if content is not None and slot.content != content:
                slot.set(content)
            return slot
        else:
            slot = GetterSetterCacheSlot(locator, self._get_content,
                                         self._set_content)
            self._referenced_cache_slots[locator] = slot
            self._referenced_cache_slots_lock.release()
            if content is not None:
                slot.set(content)
            return slot

    def _get_content(self, locator):
        """
        Gets the content associated with the given locator.
        :param locator: content identifier
        :type locator: str
        :return: the content associated to the locator or `None` if not set
        :rtype: Optional[bytearray]
        """
        return self.block_store.get(locator)

    def _set_content(self, locator, content):
        """
        Sets the given content for the given locator.

        There will be a race condition if there are concurrent requests to write
        different content for the same locator. Given that the locator is a hash
        of the contents, it is highly unlikely that such condition will occur.
        Given that `InMemoryKeepBlockCache` also suffers from this race condition,
        it is assumed that it is safe to ignore it.
        :param locator: content identifier
        :type locator: str
        :param content: the content
        :type content: bytearray
        """
        # Note: there is no do ... while loop in Python
        writing_locator_content = True
        while writing_locator_content:
            if self._get_content(locator) == content:
                # No point in writing same contents
                return

            self._writing_lock.acquire()
            writing_locator_content = locator in self._writing
            if not writing_locator_content:
                self._writing.add(locator)
                self._writing_lock.release()
            else:
                write_wait = threading.Lock()
                write_wait.acquire()
                # Use of set therefore not going to enforce an order to queued
                # writes (see method doc for justification)
                self._writing_complete_listeners[locator].add(write_wait)
                self._writing_lock.release()
                write_wait.acquire()
                # Given that the locator is a hash of the content is highly
                # likely at this point that the content has already been written

        required_space = self.block_store.calculate_stored_size(content)
        self._reserve_space_in_cache(required_space)
        self.block_store.put(locator, content)
        self._reserved_space -= required_space

        with self._writing_lock:
            self._writing.remove(locator)
        while len(self._writing_complete_listeners[locator]) > 0:
            listener = self._writing_complete_listeners[locator].pop()
            listener.release()

    def _reserve_space_in_cache(self, space):
        """
        Reserves the given amount of space in the cache. Will delete older
        entries in order to get the space.

        Will raise a `ValueError` if the given space is more than the total
        cache space
        :param space: the space that is to be reserved in the cache in bytes
        """
        if space > self.cache_max:
            raise ValueError("Cannot reserve more space in the cache than the "
                             "space available in the cache")

        with self._space_increase_lock:
            write_wait = threading.Semaphore(0)
            while self._get_spare_capacity() < space:
                delete_locator = self._cache_replacement_policy.next_to_delete(
                    self.block_store.bookkeeper)

                if delete_locator is not None:
                    assert delete_locator in [put.locator for put
                                              in self.block_store.bookkeeper.get_active()]
                    deleted = self.block_store.delete(delete_locator)
                    assert deleted
                else:
                    # Space has been allocated for content that is not written
                    # yet
                    with self._writing_lock:
                        for locator in self._writing:
                            if write_wait not in self._writing_complete_listeners[locator]:
                                self._writing_complete_listeners.append(write_wait)
                    # Wait for something to be written to cache so that it
                    # can be deleted and the space can be reclaimed
                    write_wait.acquire()

            self._reserved_space += space

    def _get_spare_capacity(self):
        """
        Gets the space capacity in the cache, defined as the maximum space minus
        the size of the block store, minus the reserved amount.

        It is possible for this to be an underestimate as the reserve space is
        not returned at exactly the same time that the content is inserted into
        the store.
        :return: the space capacity in bytes
        :rtype: int
        """
        return self.cache_max - (self.block_store.bookkeeper.get_size()
                                 + self._reserved_space)
