import logging
from abc import ABCMeta, abstractmethod
from math import ceil
from threading import Event, Lock, Condition, RLock

import lmdb

logger = logging.getLogger(__name__)


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
        self._data = dict()  # type: Dict[str, bytearray]

    def get(self, locator):
        return self._data.get(locator, None)

    def put(self, locator, content):
        self._data[locator] = content

    def delete(self, locator):
        if locator not in self._data:
            return False
        del self._data[locator]
        return True


class _BlockControl(object):
    """
    Controls access to code within a `with` block.
    """
    def __init__(self):
        self.counter = 0
        self.condition = Condition()
        self.entry_allowed = Event()
        self.entry_allowed.set()

    def __enter__(self):
        if not self.entry_allowed.is_set():
            # Prevent entry into block until allowed again
            self.entry_allowed.wait()
        with self.condition:
            self.counter += 1

    def __exit__(self, *args, **kwargs):
        with self.condition:
            self.counter -= 1
            self.condition.notify_all()


class OpenTransactionBuffer(object):
    """
    Buffer that can only be read from whilst a transaction is open.
    """
    _NO_LONGER_VALID_ERROR = IOError("The buffer is no longer accessible")

    class _BufferIterator(object):
        """
        Iterator for this type of buffer.
        """
        def __init__(self, open_transaction_buffer):
            self._open_transaction_buffer = open_transaction_buffer
            with self._open_transaction_buffer._read_block_control:
                self._open_transaction_buffer._open_transaction()
                self._buffer = self._open_transaction_buffer._buffer
                self._buffer_iter = iter(self._buffer)

        def next(self):
            with self._open_transaction_buffer._read_block_control:
                self._open_transaction_buffer._open_transaction()
                current_buffer = self._open_transaction_buffer._buffer
                if self._buffer != current_buffer:
                    raise IOError("Buffer has changed midway through iteration")
                return next(self._buffer_iter)

        def __iter__(self):
            return self

    def __init__(self, locator, transaction_opener, transaction_closer):
        """
        Constructor.
        :param locator: locator holding the buffer
        :type locator: str
        :param transaction_opener: opens the database transaction, returning
        back on object through which raw data can be fetched by a `get` method
        :type transaction_opener: callable
        :param transaction_closer: closes a given database transaction
        :type transaction_closer: callable
        """
        self.locator = locator
        self._transaction_opener = transaction_opener
        self._transaction_closer = transaction_closer
        self._transaction = None
        self._buffer = None
        self._read_block_control = _BlockControl()
        self._close_transaction_lock = Lock()
        self._close_counter = 0
        self._open_transaction_lock = Lock()
        self._stop_read_lock = Lock()

    def __del__(self):
        if self._has_open_transaction():
            self.close_transaction()

    def __getitem__(self, index):
        with self._read_block_control:
            self._open_transaction()
            return self._buffer[index]

    def __len__(self):
        with self._read_block_control:
            self._open_transaction()
            return len(self._buffer)

    def __eq__(self, other):
        with self._read_block_control:
            self._open_transaction()
            if isinstance(other, type(self)):
                return other._buffer == self._buffer
            # XXX: Loss of symmetric equality :(
            return other == self._buffer

    def __iter__(self):
        return OpenTransactionBuffer._BufferIterator(self)

    def __str__(self):
        with self._read_block_control:
            self._open_transaction()
            return str(self._buffer)

    def pause(self):
        """
        Pauses ability to read from the buffer.
        """
        with self._stop_read_lock:
            logger.debug(
                "Pausing access to buffer associated to `%s`" % self.locator)
            self._read_block_control.entry_allowed.clear()

    def resume(self):
        """
        Resumes ability to read from the buffer.
        """
        with self._stop_read_lock:
            logger.debug(
                "Resuming access to buffer associated to `%s`" % self.locator)
            self._read_block_control.entry_allowed.set()

    def close_transaction(self):
        """
        Closes the transaction through which the buffer data is accessed.
        """
        if self._has_open_transaction():
            # Copy value of close counter
            closed = self._close_counter

            with self._stop_read_lock:
                with self._close_transaction_lock:
                    # Ensures transaction was not already closed whilst waiting for lock
                    if self._close_counter == closed:
                        # Prevent addition reads from the buffer
                        self._read_block_control.entry_allowed.clear()

                        # Waits for all buffer reads to finish
                        while True:
                            self._read_block_control.condition.acquire()
                            if self._read_block_control.counter == 0:
                                break
                            self._read_block_control.condition.release()
                            logger.debug(
                                "Waiting for %d reader(s) of buffer associated to "
                                "`%s` to finish before transaction is closed"
                                % (
                                self._read_block_control.counter, self.locator))
                            # Wait for another reader to complete
                            self._read_block_control.condition.wait()

                        logger.info(
                            "Closing transaction for buffer associated to `%s`"
                            % self.locator)
                        self._transaction_closer(self._transaction)
                        self._transaction = None
                        self._read_block_control.entry_allowed.set()
                        self._read_block_control.condition.release()
                        self._close_counter += 1

    def _has_open_transaction(self):
        """
        Whether the transaction is open.
        :return: whether the transaction is open
        :rtype: bool
        """
        return self._transaction is not None

    def _open_transaction(self):
        """
        Opens the transaction required to read from the buffer.
        """
        if not self._has_open_transaction():
            with self._open_transaction_lock:
                # Ensures transaction not opened whilst waiting for lock
                if not self._has_open_transaction():
                    logger.info("Opening transaction for buffer associated to "
                                "`%s`" % self.locator)
                    assert self._transaction is None
                    self._transaction = self._transaction_opener()
                    self._buffer = self._transaction.get(self.locator)
                    if self._buffer is None:
                        raise OpenTransactionBuffer._NO_LONGER_VALID_ERROR


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

    def __init__(self, directory, max_size, max_readers):
        """
        Constructor.
        :param directory: the directory to use for the database (will create if
        does not already exist else will use pre-existing). This database must
        be used only by this store.
        :type directory: str
        :param max_size: maximum size that the database can grow to in bytes
        :type max_size: int
        :param max_readers: the maximum number of readers
        :type max_readers: int
        """
        super(LMDBBlockStore, self).__init__()
        self._database = lmdb.open(directory, writemap=True, map_size=max_size,
                                   max_readers=max_readers)
        self._max_size = max_size
        self._max_readers = max_readers
        self._buffers = dict()  # type: Dict[str, OpenTransactionBuffer]
        self._database_lock = Lock()
        self._read_transaction_rlock = RLock()
        self._reader_count = 0

    def get(self, locator):
        if locator in self._buffers:
            return self._buffers[locator]

        # Need to prevent use whilst writing
        with self._database_lock:
            logger.info("Getting value for `%s`" % locator)

            # Checks if key exists - return `None` if it doesn't
            transaction = self._open_read_transaction()
            cursor = transaction.cursor()
            key_found = cursor.set_key(locator)
            transaction.abort()
            if not key_found:
                return None

            content_buffer = OpenTransactionBuffer(
                locator, self._open_read_transaction,
                self._close_read_transaction)
            self._buffers[locator] = content_buffer
            return content_buffer

    def put(self, locator, content):
        if not isinstance(content, bytearray):
            content = bytearray(content)

        # Need to prevent new buffers being acquired via `get`
        with self._database_lock:
            # Pause and close any buffers to the content that is about to be
            # overwritten
            logger.info("Putting value of %d bytes for `%s`" % (len(content), locator))
            self._pause_and_close_buffers(locator)
            with self._database.begin(write=True) as transaction:
                transaction.put(locator, content)
            self._resume_buffers(locator)

    def delete(self, locator):
        # Need to prevent new buffers being acquired via `get`
        with self._database_lock:
            logger.info("Deleting value for `%s`" % locator)
            # Pause and close all buffers so the deleted entry is not maintained
            # in snapshot held by reader transaction
            self._pause_and_close_buffers()

            # Delete from LMDB database
            with self._database.begin(write=True) as transaction:
                deleted = transaction.delete(locator)

            # Resume buffers as transaction is complete
            self._resume_buffers()

            if locator in self._buffers:
                # Dereference existing buffers for locator, which will now be
                # invalid
                del self._buffers[locator]

            return deleted

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

    def _pause_and_close_buffers(self, locator=None):
        """
        Pauses and then closes buffers.
        :param locator: optionally only closes buffers with the given locator
        :type locator: Optional[str]
        """
        for open_transaction_buffer in self._buffers.values():
            if locator is None or open_transaction_buffer.locator == locator:
                open_transaction_buffer.pause()
                open_transaction_buffer.close_transaction()

    def _resume_buffers(self, locator=None):
        """
        Resumes the buffers.
        :param locator: optionally only resumes buffers with the given locator
        :type locator: Optional[str]
        """
        for open_transaction_buffer in self._buffers.values():
            if locator is None or open_transaction_buffer.locator == locator:
                open_transaction_buffer.resume()

    def _open_read_transaction(self):
        """
        Opens a read-only database transaction, dealing with limits on the
        maximum number of readers.

        Note: the max readers limit does not stop the opening of non-readonly
        transactions.
        :return: the opened transaction
        :rtype: Transaction
        """
        with self._read_transaction_rlock:
            if self._reader_count == self._max_readers:
                # Max number of reader transactions - closing all
                for open_transaction_buffer in self._buffers.values():
                    open_transaction_buffer.close_transaction()
                self._reader_count = 0

            self._reader_count += 1
            assert self._reader_count <= self._max_readers
            return self._database.begin(buffers=True)

    def _close_read_transaction(self, transaction):
        """
        Closes a read-only database transaction.
        :param transaction: the read-only transaction to be closed
        :type Transaction
        """
        with self._read_transaction_rlock:
            transaction.abort()
            self._reader_count -= 1
            assert self._reader_count >= 0


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