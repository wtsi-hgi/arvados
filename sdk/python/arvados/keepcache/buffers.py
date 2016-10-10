import logging
from abc import ABCMeta, abstractmethod

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


class OpeningBuffer(PseudoBuffer):
    """
    LMDB transaction buffer.
    """
    class _BufferIterator(object):
        """
        Iterator for this type of buffer.
        """
        def __init__(self, parent):
            """
            Constructor.
            :param parent: TODO
            :type: OpeningBuffer
            """
            self.parent = parent
            self._index = 0

        def next(self):
            with self.parent._transaction_lock:
                with self.parent._database.begin(buffers=True) as transaction:
                    buffer_pointer = transaction.get(self.parent.locator)
                    self.parent._raise_if_invalid_buffer(buffer_pointer)
                    if self._index == len(buffer_pointer):
                        raise StopIteration()
                    value = buffer_pointer[self._index]
                    self._index += 1
                    return value

        def __iter__(self):
            return self

    def __init__(self, locator, database, database_lock):
        """
        Constructor.
        :param locator: locator holding the buffer
        :type locator: str
        :param database: TODO
        :type: Environment
        :param database_lock: TODO
        :type: Lock
        """
        self.locator = locator
        self._database = database
        self._transaction_lock = database_lock

    def __iter__(self):
        return OpeningBuffer._BufferIterator(self)

    def __getitem__(self, index):
        with self._transaction_lock:
            with self._database.begin(buffers=True) as transaction:
                buffer_pointer = transaction.get(self.locator)
                self._raise_if_invalid_buffer(buffer_pointer)
                return buffer_pointer[index]

    def __len__(self):
        with self._transaction_lock:
            with self._database.begin(buffers=True) as transaction:
                buffer_pointer = transaction.get(self.locator)
                self._raise_if_invalid_buffer(buffer_pointer)
                return len(buffer_pointer)

    def __eq__(self, other):
        # XXX: Loss of symmetric equality :(
        if not (isinstance(other, PseudoBuffer) or isinstance(other, buffer)
                or isinstance(other, bytearray)):
            return False
        return str(other) == str(self)

    def __str__(self):
        with self._transaction_lock:
            with self._database.begin() as transaction:
                data = transaction.get(self.locator)
                self._raise_if_invalid_buffer(data)
                return str(data)

    def _raise_if_invalid_buffer(self, buffer):
        """
        Raises an error if the given buffer is invalid.
        :param buffer: the buffer retrieved from the LMDB database
        :type: Optional[buffer]
        """
        if buffer is None:
            raise RuntimeError("Value associated to `%s` is no longer in the "
                               "database" % self.locator)
