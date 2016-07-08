from abc import ABCMeta

import lmdb


class BlockStore(object):
    """
    Store for blocks.
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
    Block store backed by a Lightning Memory-Mapped Database (LMDB).
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
    Block store that records accesses and modifications to entries.
    """
    def __init__(self, block_store, block_store_usage_recorder):
        """
        Constructor.
        :param block_store: the block store to record use of
        :type block_store: BlockStore
        :param block_store_usage_recorder: recorder
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
