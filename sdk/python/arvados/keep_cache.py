import threading
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from weakref import WeakValueDictionary

import lmdb


class CacheSlot(object):
    """
    Model of a slot in the cache.
    """
    __metaclass__ = ABCMeta

    def __init__(self, locator):
        """
        Constructor.
        :param locator: identifier
        :type locator: str
        """
        self.locator = locator
        self._content = None
        self.ready = threading.Event()

    @property
    def content(self):
        """
        Gets the contents of the cache slot.
        :return: the contents of the cache slot or `None` if not set
        """
        return self._content

    def get(self):
        """
        Gets this cache slot's contents. If the contents are not set, it will
        block until they are.
        :return: the contents
        :rtype bytearray
        """
        self.ready.wait()
        return self.content

    def set(self, content):
        """
        Sets this cache slot's contents to the given value and marks slot as
        ready.
        :param content: the value to set the contents to
        :rtype: bytearray
        """
        self._content = content
        self.ready.set()

    def size(self):
        """
        The size of this slot's contents. Will return 0 if contents is `None`.
        :return: the size of the contents
        :rtype: int
        """
        if self.content is None:
            return 0
        else:
            return len(self.content)


class GetterSetterCacheSlot(CacheSlot):
    """
    Model of a slot in the cache where the contents of the slot are loaded and
    set using the given getter and setter methods.
    """
    def __init__(self, locator, content_getter, content_setter):
        """
        Constructor.
        :param locator: identifier
        :type locator: str
        :param content_getter: method that gets the contents associated to the
        given locator or `None` if the contents have not been defined
        :type content_getter: Callable[[str], Optional[bytearray]]
        :param content_setter: method that sets the contents associated to the
        given locator
        :type content_setter: Callable[[str, bytearray], None]
        """
        super(GetterSetterCacheSlot, self).__init__(locator)
        self._content_getter = content_getter
        self._content_setter = content_setter

    def get(self):
        if self.content is not None:
            return self.content
        else:
            content = self._content_getter(self.locator)
            if content is not None:
                super(GetterSetterCacheSlot, self).set(content)
                return content
            else:
                return super(GetterSetterCacheSlot, self).get()

    def set(self, content):
        self._content_setter(self.locator, content)
        super(GetterSetterCacheSlot, self).set(content)


class KeepBlockCache(object):
    """
    Base class for Keep block caches.
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


class BasicKeepBlockCache(KeepBlockCache):
    def __init__(self, cache_max=(256 * 1024 * 1024)):
        super(BasicKeepBlockCache, self).__init__(cache_max)
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


class KeepBlockCacheWithLMDB(KeepBlockCache):
    """
    TODO
    """
    # os.getenv('HOME', '/root')
    def __init__(self, cache_max=20 * 1024 * 1024 * 1024,
                 database_directory="/tmp/db"):
        """
        TODO

        The database should only be used by this cache.
        :param cache_max: maximum cache size (default: 20GB)
        """
        super(KeepBlockCacheWithLMDB, self).__init__(cache_max)
        self.database = lmdb.open(database_directory, writemap=True,
                                  map_size=self.cache_max)
        self._referenced_cache_slots = WeakValueDictionary()
        self._referenced_cache_slots_lock = threading.Lock()
        self._temp_fifo = []
        self._cache_size = self._calculate_cache_size()
        self._cache_size_lock = threading.Lock()
        self._writing = set()
        self._writing_lock = threading.Lock()
        self._writing_complete_listeners = defaultdict(set)

    @property
    def cache_size(self):
        """
        Gets the total size of the cache.
        :return: the size of the cache in bytes
        :rtype: int
        """
        return self._cache_size

    def get(self, locator):
        # Return pre-existing model of slot if referenced elsewhere. This is
        # required to make the cache slot's blocking `get` method work in the
        # same was as it does with the in-memory cache. It also avoids wasting
        # memory with multiple models of the same slot.
        slot = self._referenced_cache_slots.get(locator, None)
        if slot is not None:
            return slot
        else:
            with self.database.begin(buffers=True) as transaction:
                content = transaction.get(locator)
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
        assert self.cache_size <= self.cache_max

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

    def _calculate_cache_size(self):
        """
        Calculates the size of this cache by (expensively) iterating through all
        values in the database.
        :return: the size (in bytes) of the cache
        :rtype: int
        """
        # FIXME: This is a horrific way of caculating this
        total_size = 0
        with self.database.begin() as transaction:
            cursor = transaction.cursor()
            for key, value in cursor:
                total_size += len(value)
                # FIXME: temp only
                self._temp_fifo.append(key)
        return total_size

    def _get_content(self, locator):
        """
        Gets the content associated with the given locator.
        :param locator: content identifier
        :type locator: str
        :return: the content associated to the locator or `None` if not set
        :rtype: Optional[bytearray]
        """
        with self.database.begin(buffers=True) as transaction:
            return transaction.get(locator)

    def _set_content(self, locator, content):
        """
        Sets the given content for the given locator.

        There will be a race condition if there are concurrent requests to write
        different content for the same locator. Given that the locator is a hash
        of the contents, it is highly unlikely that such condition will occur.
        Given that `BasicKeepBlockCache` also suffers from this race condition,
        it is assumed that it is safe to ignore it.
        :param locator: content identifier
        :type locator: str
        :param content: the content
        :type content: bytearray
        """
        # No do ... while loop in Python
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

        self._reserve_space_in_cache(len(content))
        with self.database.begin(write=True, buffers=True) as transaction:
            transaction.put(locator, content)
        self._temp_fifo.append(locator)

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
                             "size of the cache")

        with self._cache_size_lock:
            spare_capacity = self.cache_max - self.cache_size
            assert spare_capacity >= 0
            if spare_capacity < space:
                self._free_space_in_cache(space - spare_capacity)
                assert self.cache_max - self.cache_size >= space
            self._cache_size += space

    def _free_space_in_cache(self, space):
        """
        Frees the given amount of space in the cache by deleting entries.

        This method cannot guarantee that the space will be free at the point of
        return: external control of this method is required for that.
        :param space: the amount of space to free (in bytes)
        """
        write_wait = threading.Semaphore(0)
        with self.database.begin(write=True, buffers=True) as transaction:
            while self.cache_size < space:
                if len(self._temp_fifo) > 0:
                    locator = self._temp_fifo.pop(0)
                    content = transaction.get(locator)
                    transaction.delete(locator)
                    self._cache_size -= len(content)
                else:
                    # Space has been allocated for content that is not written
                    # yet
                    with self._writing_lock:
                        for locator in self._writing:
                            if write_wait not in self._writing_complete_listeners[locator]:
                                self._writing_complete_listeners.append(write_wait)
                    # Wait for something to be written to cache so that it can
                    # be deleted and the space can be reclaimed
                    write_wait.acquire()
