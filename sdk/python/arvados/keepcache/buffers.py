import logging
from abc import ABCMeta
from abc import abstractmethod
from thread import get_ident
from threading import Lock, Condition

_logger = logging.getLogger(__name__)


class PseudoBuffer(object):
    """
    Looks and smells like a Python 2 `buffer` object.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __getitem__(self, index):
        """
        Gets the item with the given index from this buffer.

        Raises `IndexError` if index is out of bounds.
        :param index: the index of the item to get
        :type index: int
        :return: the item at the given index
        :rtype: byte
        """

    @abstractmethod
    def __len__(self):
        """
        Gets the length of this buffer.
        :return: the length of this buffer
        :rtype: int
        """

    @abstractmethod
    def __iter__(self):
        """
        Gets an iterator for this buffer.
        :return: the iterator
        :rtype: iter
        """


class _BlockControl(object):
    """
    Controls access to code within a `with` block.
    """
    def __init__(self):
        self.exit_condition = Condition()
        self.entry_lock = Lock()
        # "Bag" of threads in block
        self._entries_in_block = list()   # type: List[int]

    def __enter__(self):
        # Note: This looks similar to what an RLock does. However, RLocks can
        # only be released by threads that hold the lock. This does not allow
        # a third-party thread to control new threads from entering the block
        thread_id = get_ident()
        if thread_id not in self._entries_in_block:
            with self.entry_lock:
                self._entries_in_block.append(thread_id)

    def __exit__(self, *args, **kwargs):
        thread_id = get_ident()
        assert thread_id in self._entries_in_block
        self._entries_in_block.remove(thread_id)
        with self.exit_condition:
            self.exit_condition.notify_all()

    @property
    def counter(self):
        """
        Gets the number of entries in the block.
        :return: number of entries in block
        """
        return len(self._entries_in_block)


class OpenTransactionBuffer(PseudoBuffer):
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
        self._change_transaction_lock = Lock()
        self._stop_read_lock = Lock()
        self._close_counter = 0
        self._paused = False

    def __del__(self):
        if self._has_open_transaction():
            _logger.debug("Destructor called for buffer associated with %s"
                          % self.locator)
            self.close_transaction()

    def __iter__(self):
        return OpenTransactionBuffer._BufferIterator(self)

    def __getitem__(self, index):
        with self._read_block_control:
            self._open_transaction()
            assert self._buffer is not None
            return self._buffer[index]

    def __len__(self):
        with self._read_block_control:
            self._open_transaction()
            assert self._buffer is not None
            return len(self._buffer)

    def __eq__(self, other):
        with self._read_block_control:
            self._open_transaction()
            assert self._buffer is not None
            if isinstance(other, type(self)):
                return other._buffer == self._buffer
            # XXX: Loss of symmetric equality :(
            return other == self._buffer

    def __str__(self):
        with self._read_block_control:
            self._open_transaction()
            assert self._buffer is not None
            return str(self._buffer)

    def pause(self):
        """
        Pauses ability to read from the buffer and waits for all current
        readers to finish.
        """
        with self._stop_read_lock:
            if not self._paused:
                _logger.debug("Pausing access to buffer associated to `%s`"
                              % self.locator)
                self._read_block_control.entry_lock.acquire()

                # Waits for readers to finish
                while self._read_block_control.counter > 0:
                    _logger.debug(
                        "Waiting for %d reader(s) of buffer associated "
                        "to `%s` to finish before transaction is closed"
                        % (self._read_block_control.counter, self.locator))
                    self._read_block_control.exit_condition.acquire()
                    self._read_block_control.exit_condition.wait()
                assert self._read_block_control.counter == 0
                self._paused = True
                _logger.debug("All readers finished - buffer for %s is paused"
                              % self.locator)

    def resume(self):
        """
        Resumes ability to read from the buffer.
        """
        with self._stop_read_lock:
            if self._paused:
                _logger.debug("Resuming access to buffer associated to `%s`"
                              % self.locator)
                self._paused = False
                self._read_block_control.entry_lock.release()

    def close_transaction(self):
        """
        Closes the transaction through which the buffer data is accessed.
        Buffer will be paused once closed.
        """
        if self._has_open_transaction():
            with self._change_transaction_lock:
                if self._has_open_transaction():
                    if not self._paused:
                        self.pause()
                    _logger.debug(
                        "Closing transaction for buffer associated to `%s`"
                        % self.locator)
                    self._transaction_closer(self._transaction)
                    self._transaction = None
                    self._buffer = None
                _logger.debug("Closed buffer assocaited to `%s`"
                              % self.locator)

    def _open_transaction(self):
        """
        Opens the transaction required to read from the buffer.
        """
        if not self._has_open_transaction():
            with self._change_transaction_lock:
                # Ensures transaction not opened whilst waiting for lock
                if not self._has_open_transaction():
                    _logger.debug("Opening transaction for buffer associated "
                                 "to `%s`" % self.locator)
                    assert self._transaction is None
                    assert self._buffer is None
                    self._transaction = self._transaction_opener()
                    self._buffer = self._transaction.get(self.locator)
                    if self._buffer is None:
                        raise OpenTransactionBuffer._NO_LONGER_VALID_ERROR
                    # Resume any paused access
                    self.resume()

    def _has_open_transaction(self):
        """
        Whether the transaction is open.
        :return: whether the transaction is open
        :rtype: bool
        """
        return self._transaction is not None and self._buffer is not None
