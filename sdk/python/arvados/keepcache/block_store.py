from abc import ABCMeta, abstractmethod

import lmdb
from math import ceil


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

    @abstractmethod
    def calculate_stored_size(self, content):
        """
        Calculates how much size the given content will take up when stored
        inside this block store.
        :param content: the content
        :type content: bytearray
        :return: size of content when stored in bytes
        :rtype: int
        """


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

    def calculate_stored_size(self, content):
        return len(content)


class LMDBBlockStore(BlockStore):
    """
    Block store backed by a Lightning Memory-Mapped Database (LMDB).
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
        self._database = lmdb.open(directory, writemap=True, map_size=map_size)
        self._map_size = map_size

    def get(self, locator):
        with self._database.begin() as transaction:
            return transaction.get(locator)

    def put(self, locator, content):
        with self._database.begin(write=True) as transaction:
            transaction.put(locator, content)

    def delete(self, locator):
        with self._database.begin(write=True) as transaction:
            return transaction.delete(locator)

    def calculate_stored_size(self, content):
        # TODO: if max_key_size + size <= 2040, LMDB will store multiple entries
        # per page. This implementation currently assumes one entry per page at
        # the expense of a small amount of wasted space. Given that most Keep
        # blocks will be large, the loss will be relatively small
        size = len(content)
        page_size = self._get_page_size()
        max_key_size = self._database.max_key_size()
        return int(ceil(float(LMDBBlockStore._HEADER_SIZE + max_key_size + size) / float(page_size)) * page_size)

    def calculate_usuable_size(self):
        """
        Calculates the usable size of this block store.
        :return: the usable size in bytes
        :rtype: int
        """
        page_size = self._get_page_size()
        # Note: This is an underestimate as it assumes all content will be at
        # least the size of a page
        size = self._map_size - (4 * page_size + LMDBBlockStore._HEADER_SIZE)
        assert self._map_size >= page_size >= 0
        return size

    def _get_page_size(self):
        """
        Gets the size of a page.
        :return: the page size in bytes
        :rtype: int
        """
        return self._database.stat()["psize"]


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
        self.bookkeeper.record_put(locator, len(content))
        return self._block_store.put(locator, content)

    def delete(self, locator):
        return_value = self._block_store.delete(locator)
        # Better to think things are in the store rather than not
        self.bookkeeper.record_delete(locator)
        return return_value

    def calculate_stored_size(self, content):
        return self._block_store.calculate_stored_size(content)
