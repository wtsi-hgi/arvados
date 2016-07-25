import os
from abc import ABCMeta, abstractmethod
from base64 import urlsafe_b64encode
from collections import defaultdict
from math import ceil
from threading import Thread, Event
from weakref import WeakValueDictionary

import lmdb
import rocksdb


class BlockStore(object):
    """
    Store for blocks.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, locator):
        """
        Gets the block with the given locator from this store.
        :param locator: the identifier of the block to get
        :type locator: str
        :return: the block else `None` if not found
        :rtype: Optional[bytearray]
        """

    @abstractmethod
    def put(self, locator, content):
        """
        Puts the given content into this store for the block with the given
        locator.
        :param locator: the identifier of the block
        :type locator: str
        :param content: the block content
        :type content: bytearray
        """

    @abstractmethod
    def delete(self, locator):
        """
        Deletes the block with the given locator from this store. No-op if the
        block does not exist.
        :param locator: the block identifier
        :type locator: str
        :return: whether an entry was deleted
        :rtype: bool
        """

    def calculate_stored_size(self, content):
        """
        Calculates how much size the given content will take up when stored
        inside this block store.
        :param content: the content
        :type content: bytearray
        :return: size of content when stored in bytes
        :rtype: int
        """
        # Basic calculation that should be overriden if required
        return len(content)


class InMemoryBlockStore(BlockStore):
    """
    Basic in-memory block store.
    """
    def __init__(self):
        self._data = dict()     # type: Dict[str, bytearray]

    def get(self, locator):
        return self._data.get(locator, None)

    def put(self, locator, content):
        self._data[locator] = content

    def delete(self, locator):
        if locator not in self._data:
            return False
        del self._data[locator]
        return True


class DiskOnlyBlockStore(BlockStore):
    """
    Blocks store that writes blocks to disk without any fanciness.
    """
    def __init__(self, directory):
        """
        Constructor.
        :param directory: the directory to write blocks to (will be created if
        does not exist)
        :type directory: str
        """
        if not os.path.exists(directory):
            os.makedirs(directory)
        self._directory = directory

    def get(self, locator):
        path = self._get_path(locator)
        if not os.access(path, os.R_OK):
            return None
        return open(path, "r").read()

    def put(self, locator, content):
        with open(self._get_path(locator), "w+") as file:
            file.write(content)

    def delete(self, locator):
        path = self._get_path(locator)
        if not os.access(path, os.R_OK):
            return False
        os.remove(path)
        return True

    def _get_path(self, locator):
        """
        Gets the path to the file related to the given locator.
        :param locator: the locator
        :type locator: str
        :return: the file path
        :rtype: str
        """
        # Translation from untrusted locator to safe file name
        locator = urlsafe_b64encode(locator)
        return os.path.join(self._directory, locator)


class BufferedBlockStore(BlockStore):
    """
    Block store that returns buffers to data.
    """
    __metaclass__ = ABCMeta

    class TrackedBuffer():
        """
        A (pseudo) buffer, which can be invalidated and modified "remotely".
        """
        def __init__(self, underlying_buffer):
            """
            Constructor.
            :param underlying_buffer: the underlying, actual buffer
            :type underlying_buffer: buffer
            """
            self.underlying_buffer = None
            self._valid = False
            self.set(underlying_buffer)

        @property
        def valid(self):
            """
            Whether the buffer is still valid.
            :return: the validity of the buffer
            :rtype: bool
            """
            return self._valid

        def set(self, underlying_buffer):
            """
            Sets the underlying buffer, setting it to valid.
            :param underlying_buffer: the underlying buffer
            """
            self.underlying_buffer = underlying_buffer
            self._valid = True

        def invalidate(self):
            """
            Invalidates the buffer.
            """
            self._valid = False
            # Throw away reference to underlying buffer to close it
            self.underlying_buffer = None

        def __getattr__( self, name):
            # Used to make this object act in the same way as the underlying buffer
            if not self._valid:
                raise IOError("The buffer cannot be used as it has been invalidated")
            assert self.underlying_buffer is not None
            return self.underlying_buffer.__getattribute__(name)

        def __eq__(self, other):
            if isinstance(other, type(self)):
                return other.underlying_buffer == self.underlying_buffer \
                       and other.valid == self.valid
            # XXX: Loss of symmetric equality :(
            return other == self.underlying_buffer

    def __init__(self):
        """
        Constructor.
        """
        self._active_buffers = WeakValueDictionary()  # type: Dict[str, BufferedBlockStore.TrackedBuffer]

    def get(self, locator):
        active_buffer = self._active_buffers.get(locator, None)
        return active_buffer if active_buffer is not None and active_buffer.valid else None

    def put(self, locator, content):
        active_buffer = self._active_buffers.get(locator)
        if active_buffer is not None:
            if active_buffer.underlying_buffer != content:
                # Content overwritten - reload buffer
                active_buffer.set(content)
        else:
            self.track(locator, content)

    def delete(self, locator):
        active_buffer = self._active_buffers.get(locator)
        if active_buffer is not None and active_buffer.valid:
            # Invalid existing buffer so that old copy can be cleaned up
            active_buffer.invalidate()

    def track(self, locator, content):
        """
        Tracks pointers to the content associated to the given locator.
        :param locator: the content's locator
        :type locator: str
        :param content: the content to track
        :type content: buffer
        :return: the content in a form that can be tracked
        :rtype: BufferedBlockStore.TrackedBuffer
        """
        tracked_content = BufferedBlockStore.TrackedBuffer(content)
        self._active_buffers[locator] = tracked_content
        return tracked_content


class LMDBBlockStore(BufferedBlockStore):
    """
    Block store backed by Lightning Memory-Mapped Database (LMDB).
    """
    _HEADER_SIZE = 16

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
        super(LMDBBlockStore, self).__init__()
        self._database = lmdb.open(directory, writemap=True, map_size=map_size)
        self._map_size = map_size
        self._readers = set()   # type: Set[Thread]

    def get(self, locator):
        existing_buffer = super(LMDBBlockStore, self).get(locator)
        if existing_buffer is not None:
            return existing_buffer

        read_event = Event()
        content = []

        def read():
            with self._database.begin(buffers=True) as transaction:
                # Strange use of list instead of `nonlocal` due to use of
                # version of Python that was outmoded in 2008...
                # http://stackoverflow.com/questions/3190706/nonlocal-keyword-in-python-2-x
                content.append(transaction.get(locator))
            read_event.set()

        reader = Thread(target=read)
        self._readers.add(reader)
        reader.start()
        read_event.wait()
        self._readers.remove(reader)

        if len(content) == 0 or content[0] is None:
            return None
        else:
            assert len(content) == 1
            content = content[0]
        return super(LMDBBlockStore, self).track(locator, content)

    def put(self, locator, content):
        with self._database.begin(write=True) as transaction:
            transaction.put(locator, content)
        super(LMDBBlockStore, self).put(locator, content)

    def delete(self, locator):
        super(LMDBBlockStore, self).delete(locator)
        with self._database.begin(write=True) as transaction:
            return transaction.delete(locator)

    def calculate_stored_size(self, content):
        # Note: if max_key_size + size <= 2040, LMDB will store multiple entries
        # per page. This implementation currently assumes one entry per page at
        # the expense of a small amount of wasted space. Given that most Keep
        # blocks will be large, the loss will be relatively small.
        size = len(content)
        page_size = self._get_page_size()
        max_key_size = self._database.max_key_size()
        return int(ceil(float(LMDBBlockStore._HEADER_SIZE + max_key_size + size)
                        / float(page_size)) * page_size)

    def calculate_usuable_size(self):
        """
        Calculates the usable size of this block store.
        :return: the usable size in bytes
        :rtype: int
        """
        page_size = self._get_page_size()
        # Fixed cost (e.g. the pointer to the data root, free list root, etc)
        fixed_cost = 4 * page_size + LMDBBlockStore._HEADER_SIZE
        size = self._map_size - fixed_cost
        assert self._map_size >= size >= 0
        return size

    def _get_page_size(self):
        """
        Gets the size of a page.
        :return: the page size in bytes
        :rtype: int
        """
        return self._database.stat()["psize"]


class RocksDBBlockStore(BlockStore):
    """
    Block store backed by RocksDB.
    """
    def __init__(self, directory, rocksdb_options=None):
        """
        Constructor.
        :param directory: location used to store files related to the database
        :type directory: str
        :param rocksdb_options: options to use with RockDB database (defaults to
        creating the database if missing)
        :type rocksdb_options: rocksdb.Options
        """
        if rocksdb_options is None:
            rocksdb_options = rocksdb.Options(create_if_missing=True)
        self._database = rocksdb.DB(directory, rocksdb_options)

    def get(self, locator):
        content = self._database.get(locator)
        if content is not None:
            content = bytearray(content)
        return content

    def put(self, locator, content):
        self._database.put(locator, bytes(content))

    def delete(self, locator):
        batch = rocksdb.WriteBatch()
        existed = self._database.get(locator) is not None
        self._database.delete(locator)
        deleted = self._database.get(locator) is None
        self._database.write(batch)
        return existed and deleted

    def calculate_stored_size(self, content):
        # FIXME: This won't be accurate as there will be additional information
        # stored.
        return len(content)


class BookkeepingBlockStore(BlockStore):
    """
    Block store that uses a bookkeeper to record accesses and modifications to
    entries in an underlying block store.
    """
    def __init__(self, block_store, bookkeeper):
        """
        Constructor.
        :param block_store: the block store to record use of
        :type block_store: BlockStore
        :param bookkeeper: bookkeeper
        :type bookkeeper: BlockStoreBookkeeper
        """
        self._block_store = block_store
        self.bookkeeper = bookkeeper

    def get(self, locator):
        self.bookkeeper.record_get(locator)
        return self._block_store.get(locator)

    def put(self, locator, content):
        stored_size = self.calculate_stored_size(content)
        self.bookkeeper.record_put(locator, stored_size)
        return self._block_store.put(locator, content)

    def delete(self, locator):
        return_value = self._block_store.delete(locator)
        # Better to think things are in the store rather than not
        self.bookkeeper.record_delete(locator)
        return return_value

    def calculate_stored_size(self, content):
        return self._block_store.calculate_stored_size(content)
