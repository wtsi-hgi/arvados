import threading
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import defaultdict
from datetime import datetime
from weakref import WeakValueDictionary

import lmdb
from sqlalchemy import Column, DateTime, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class BlockStoreUsageRecorder:
    """
    Recorder for the usage of a block store.
    """
    __metaclass__ = ABCMeta

    class Record:
        """
        TODO
        """
        @abstractproperty
        def locator(self):
            """
            TODO
            :return:
            """

        @abstractproperty
        def timestamp(self):
            """
            TODO
            :return:
            """

    class PutRecord(Record):
        """
        TODO
        """
        @abstractproperty
        def size(self):
            """
            TODO
            :return:
            """

    class GetRecord(Record):
        """
        TODO
        """

    class DeleteRecord(Record):
        """
        TODO
        """

    def get_size(self):
        """
        Gets the current (known) size of the block store.
        :return: the current size of the block store in bytes
        :rtype: int
        """

    def record_get(self, locator):
        """
        Records an access to the block in the store with the given locator.
        :param locator: the block identifier
        """

    def record_put(self, locator, content_size):
        """
        Records the writing of content of the given size as the block with the
        given locator
        :param locator: the block identifier
        :type locator: str
        :param content_size: the size of the block content
        :type content_size: int
        """

    def record_delete(self, locator):
        """
        Records the deletion of the block with the given locator.
        :param locator: the block identifier
        :type locator: str
        """

    def _get_current_timestamp(self):
        """
        Gets the current timestamp.
        :return: the current timestamp
        :rtype: datetime
        """
        return datetime.now()


class DatabaseBlockStoreUsageRecorder(BlockStoreUsageRecorder):
    """
    Recorder for usage of a block store where records are kept in a database.
    """
    SQLAlchemyModel = declarative_base()

    class _Record(SQLAlchemyModel, BlockStoreUsageRecorder.Record):
        __abstract__ = True
        __tablename__ = BlockStoreUsageRecorder.Record.__name__
        locator = Column(String, primary_key=True)
        timestamp = Column(DateTime)

    class _PutRecord(_Record, BlockStoreUsageRecorder.PutRecord):
        __tablename__ = BlockStoreUsageRecorder.PutRecord.__name__
        size = Column(Integer)

    class _GetRecord(_Record, BlockStoreUsageRecorder.GetRecord):
        __tablename__ = BlockStoreUsageRecorder.GetRecord.__name__

    class _DeleteRecord(_Record, BlockStoreUsageRecorder.DeleteRecord):
        __tablename__ = BlockStoreUsageRecorder.DeleteRecord.__name__

    def __init__(self, database_location):
        """
        Constructor.
        :param database_location: the location of the database
        :type database_location: str
        """
        engine = create_engine(database_location)
        DatabaseBlockStoreUsageRecorder.SQLAlchemyModel.metadata.create_all(
            bind=engine)
        Session = sessionmaker(bind=engine)
        self._database = Session()

    def get_size(self):
        pass

    def record_get(self, locator):
        record = self._create_record(
            DatabaseBlockStoreUsageRecorder._GetRecord, locator)
        self._store(record)

    def record_put(self, locator, content_size):
        record = self._create_record(
            DatabaseBlockStoreUsageRecorder._PutRecord, locator)
        record.size = content_size
        self._store(record)

    def record_delete(self, locator):
        record = self._create_record(
            DatabaseBlockStoreUsageRecorder._DeleteRecord, locator)
        self._store(record)

    def _create_record(self, cls, locator):
        """
        TODO
        :param cls:
        :param locator:
        :return:
        """
        record = cls()
        record.locator = locator
        record.timestamp = self._get_current_timestamp()
        return record

    def _store(self, record):
        """
        TODO
        :param record:
        :return:
        """
        self._database.add(record)
        self._database.commit()


class BlockStore:
    """
    Simple store for blocks.
    """
    __metaclass__ = ABCMeta

    def get(self, locator):
        """
        Gets the block with the given locator from this store.
        :param locator: the identifier of the block to get
        :type locator: str
        :return: the block else `None` if not found
        """

    def put(self, locator, content):
        """
        Puts the given content into this store for the block with the given
        locator.
        :param locator: the identifier of the block
        :type locator: str
        :param content: the block content
        :type content: bytearray
        """

    def delete(self, locator):
        """
        Deletes the block with the given locator from this store. No-op if the
        block does not exist.
        :param locator: the block identifier
        :type locator: str
        """


class LMDBBlockStore(BlockStore):
    """
    Simple block store backed by a Lightning Memory-Mapped Database (LMDB).
    """
    def __init__(self, directory, map_size):
        """
        Constructor.
        :param directory: the directory to use for the database (will create if
        does not already exist else will use pre-existing). This database must
        be used only by this store.
        :type directory: str
        :param map_size: maximum size that the database can grow to in bytes
        :type map_size: int
        """
        self._database = lmdb.open(directory, writemap=True, map_size=map_size)

    def get(self, locator):
        with self._database.begin(buffers=True) as transaction:
            return transaction.get(locator)

    def put(self, locator, content):
        with self._database.begin(write=True) as transaction:
            transaction.put(locator, content)

    def delete(self, locator):
        with self._database.begin(write=True) as transaction:
            transaction.delete(locator)


class RecordingBlockStore(BlockStore):
    """
    Block store that records accesses and modifications to entries in a block
    store.
    """
    def __init__(self, block_store, block_store_usage_recorder):
        """
        Constructor.
        :param block_store: the block store to record use of
        :type block_store: BlockStore
        :param block_store_usage_recorder: TODO
        :type block_store_usage_recorder: BlockStoreUsageRecorder
        """
        self._block_store = block_store
        self.recorder = block_store_usage_recorder

    def get(self, locator):
        self.recorder.record_get(locator)
        return self._block_store.get(locator)

    def put(self, locator, content):
        self.recorder.record_put(locator, len(content))
        return self._block_store.put(locator, content)

    def delete(self, locator):
        return_value = self._block_store.delete(locator)
        # Better to think things are in the store rather than not
        self.recorder.record_delete(locator)
        return return_value


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
        self.ready = threading.Event()
        self._content = None

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


class KeepBlockCacheWithBlockStore(KeepBlockCache):
    """
    Keep block cache backed by a block store, which can hold blocks in any way.
    """
    def __init__(self, block_store, cache_max=20 * 1024 * 1024 * 1024):
        """
        Constructor.
        :param block_store: store for blocks
        :type block_store: RecordingBlockStore
        :param cache_max: maximum cache size (default: 20GB)
        """
        super(KeepBlockCacheWithBlockStore, self).__init__(cache_max)
        self._block_store = block_store
        self._referenced_cache_slots = WeakValueDictionary()
        self._referenced_cache_slots_lock = threading.Lock()
        self._writing = set()
        self._writing_lock = threading.Lock()
        self._writing_complete_listeners = defaultdict(set)
        self._reserved_space = 0
        self._space_increase_lock = threading.Lock()
        self._temp_fifo = []

    def get(self, locator):
        # Return pre-existing model of slot if referenced elsewhere. This is
        # required to make the cache slot's blocking `get` method work in the
        # same was as it does with the in-memory cache. It also avoids wasting
        # memory with multiple models of the same slot.
        slot = self._referenced_cache_slots.get(locator, None)
        if slot is not None:
            return slot
        else:
            content = self._block_store.get(locator)
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
        assert self._block_store.recorder.get_size() <= self.cache_max

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
        return self._block_store.get(locator)

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
        self._block_store.put(locator, content)
        self._reserved_space -= len(content)
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

        with self._space_increase_lock:
            write_wait = threading.Semaphore(0)
            while self._get_spare_capacity() < space:
                if len(self._temp_fifo) > 0:
                    locator = self._temp_fifo.pop(0)
                    self._block_store.delete(locator)
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
        return self.cache_max - (self._block_store.recorder.get_size()
                                 + self._reserved_space)


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