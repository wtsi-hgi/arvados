import logging
import os
from abc import ABCMeta, abstractmethod
from errno import EEXIST
from math import ceil
from threading import Lock
from time import sleep

import lmdb

from arvados.keepcache._common import to_bytes
from arvados.keepcache._locks import GlobalLock
from arvados.keepcache.buffers import OpeningBuffer

_logger = logging.getLogger(__name__)


class BlockStore(object):
    """
    Store for blocks.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        Constructor.
        """
        self._exclusive_write_access = Lock()

    @property
    def exclusive_write_access(self):
        """
        Gets lock that can be acquired to get exclusive access to the block
        store.
        :return: write access lock
        :rtype: Lock
        """
        return self._exclusive_write_access

    @abstractmethod
    def exists(self, locator):
        """
        Gets whether this block store contains a block with the given locator.
        :param locator: the block locator
        :type locator: str
        :return: whether the block exists
        :rtype: None
        """

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
        super(InMemoryBlockStore, self).__init__()
        self._data = dict()  # type: Dict[str, bytearray]

    def exists(self, locator):
        return locator in self._data

    def get(self, locator):
        return self._data.get(locator, None)

    def put(self, locator, content):
        self._data[locator] = content

    def delete(self, locator):
        if locator not in self._data:
            return False
        del self._data[locator]
        return True


class LMDBBlockStore(BlockStore):
    """
    Block store backed by Lightning Memory-Mapped Database (LMDB).

    TLDR: LMDB does not like being a cache so has to be restrained with locks.
    If you want to put 10GB of data in the block store, make LMDB 20GB.

    LMDB uses the concept of Multi-Version Concurrency Control (MVCC) to reduce
    its use of locks. When read transactions are started, they get a snapshot
    view of the database, which is isolated against additional writes. The
    consequence of this is that data is only purged when it no longer exists in
    any snapshot; wedged read transactions can cause the database to get much
    larger than naively expected.

    LMDB requires its maximum size to be defined upon creation; if this size is
    exceeded, an error will be raised. Unless the size of disk exceeds the total
    size of all data that is going to be put in the cache (i.e. all data can fit
    in the cache at the same time), users of LMDB have to worry about the total
    size of the database. This will inevitably involve the use of locks, which
    ultimately reverts the work of LMDB's creators to remove the need for locks.
    (It suggests that LMDB is intended to be able to grow big enough to store
    all data and is therefore a questionable choice for a cache...).

    To add to the woes of using LMDB: in order to freely write and re-write
    data, empirical evidence strongly suggests that only 50% of the size of an
    LMDB database is usable in the worst case. This is not explained in any of
    the documentation surrounding LMDB; instead the reason is obfuscated in the
    10000+ lines of C that implement it:
    https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/mdb.c.
    """
    _HEADER_SIZE = 16

    # FIXME: Just accept LMDB database
    def __init__(self, directory, max_size, max_readers=126,
                 max_spare_transactions=2):
        """
        Constructor.
        :param directory: the directory to use for the database (will create if
        does not already exist else will use pre-existing). This database must
        be used only by this store.
        :type directory: str
        :param max_size: maximum size that the database can grow to in bytes
        :type max_size: int
        :param max_readers: the maximum number of readers. An error will be
        raised if the supply of readers is extinguished. LMDB's creator states
        that a thread may only "have" one transaction at a time
        (http://www.openldap.org/lists/openldap-devel/201409/msg00001.html).
        Therefore this should be set to the maximum number of threads that can
        access the database
        :type max_readers: int
        :param max_spare_transactions: maximum number of transactions to keep
        close to hand
        :type max_spare_transactions: int
        """
        super(LMDBBlockStore, self).__init__()

        if not os.path.exists(directory):
            # Surround in try to cope with race condition
            try:
                os.mkdir(directory)
            except OSError as e:
                # In Python 3 (i.e. the current version of Python as of the
                # last ~8 years) we could just use `FileExistsError`...
                if e.errno != EEXIST:
                    raise e

        self._exclusive_write_access = GlobalLock(
            os.path.join(directory, "access.lock"))
        with self._exclusive_write_access:
            self._database = lmdb.open(
                directory, writemap=True, map_size=max_size,
                max_readers=max_readers, max_spare_txns=max_spare_transactions)
        self._max_size = max_size
        self._transaction_lock = Lock()

    def exists(self, locator):
        locator = to_bytes(locator)
        with self._transaction_lock:
            with self._database.begin() as transaction:
                with transaction.cursor() as cursor:
                    return cursor.set_key(locator)

    def get(self, locator):
        _logger.debug("Getting value buffer for `%s` in LMDB" % locator)

        if not self.exists(locator):
            _logger.debug("No value for `%s` in LMDB" % locator)
            return None

        content_buffer = OpeningBuffer(
            to_bytes(locator), self._database, self._transaction_lock)
        return content_buffer

    def put(self, locator, content):
        locator = to_bytes(locator)
        if not isinstance(content, bytearray):
            content = bytearray(content)
        _logger.debug("Putting value of %d bytes for `%s` in LMDB"
                      % (len(content), locator))

        # TODO: Transaction lock required for write transaction?
        with self._transaction_lock:
            with self._database.begin(write=True) as transaction:
                transaction.put(locator, content)

    def delete(self, locator):
        locator = to_bytes(locator)

        _logger.debug("Deleting value for `%s` in LMDB" % locator)
        # TODO: Transaction lock required for write transaction?
        with self._transaction_lock:
            with self._database.begin(write=True) as transaction:
                deleted = transaction.delete(locator)

        return deleted

    def calculate_stored_size(self, content):
        # Note: if max_key_size + size <= 2040, LMDB will store multiple
        # entries per page. This implementation currently assumes one entry per
        # page at the expense of a small amount of wasted space. Given that
        # most Keep blocks will be large, the loss will be relatively small.
        size = len(content)
        page_size = self._get_page_size()
        max_key_size = self._database.max_key_size()
        return int(ceil(float(LMDBBlockStore._HEADER_SIZE + max_key_size + size)
                        / float(page_size)) * page_size)

    def calculate_usable_size(self):
        """
        Calculates the usable size of this block store.
        :return: the usable size in bytes
        :rtype: int
        """
        # TODO: Ignoring space that cannot be used because it has been "wedged"
        # open by readers
        page_size = self._get_page_size()
        # Fixed cost (e.g. the pointer to the data root, free list root, etc)
        fixed_cost = 4 * page_size + LMDBBlockStore._HEADER_SIZE
        size = self._max_size - fixed_cost
        assert self._max_size >= size >= 0
        return size

    def _get_page_size(self):
        """
        Gets the size of a page.
        :return: the page size in bytes
        :rtype: int
        """
        return self._database.stat()["psize"]


class LoadCommunicationBlockStore(BlockStore):
    """
    Block store where communication of block loads is shared across accessors
    to the block store.
    """
    DEFAULT_POLL_PERIOD = 0.5

    def __init__(self, block_store, block_load_manager,
                 poll_period=DEFAULT_POLL_PERIOD):
        """
        Constructor.
        :param block_store: the block store on which load communications are
        enabled
        :type block_store: BlockStore
        :param block_load_manager: manages what processes are loading what
        blocks
        :type block_load_manager: BlockLoadManager
        :param poll_period: the poll period used when checking if data is in
        the block store
        :type poll_period: float
        """
        super(LoadCommunicationBlockStore, self).__init__()
        self._block_store = block_store
        self._block_load_manager = block_load_manager
        self.poll_period = poll_period

    def __getattr__(self, name):
        return self._block_store.__getattribute__(name)

    def delete(self, locator):
        return self._block_store.delete(locator)

    def exists(self, locator):
        return self._block_store.exists(locator)

    def get(self, locator):
        data = self._block_store.get(locator)
        if data is None and locator in self._block_load_manager.pending:
            self._wait_for_load(locator)
            data = self._block_store.get(locator)
        return data

    def put(self, locator, content):
        return self._block_store.put(locator, content)

    def _wait_for_load(self, locator):
        """
        Waits for the block with the given locator to be loaded into the block
        store. The block store is therefore responsible for timeouts.

        Beware that on return there is no guarantee that a block will be in the
        store.
        :param locator: the block locator
        :type locator: str
        """
        _logger.info(
            "Waiting for other process to load block associated with "
            "locator `%s`" % locator)
        while True:
            if self._block_store.exists(locator):
                _logger.info("Other processes has loaded block associated "
                             "with locator `%s`" % locator)
                break
            elif locator not in self._block_load_manager.pending:
                _logger.info("No longer anyone loading content for locator "
                             "`%s`" % locator)
                break
            else:
                sleep(self.poll_period)


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
        super(BookkeepingBlockStore, self).__init__()
        self._block_store = block_store
        self.bookkeeper = bookkeeper

    def exists(self, locator):
        return self._block_store.exists(locator)

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
