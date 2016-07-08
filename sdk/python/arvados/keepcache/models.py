from abc import abstractproperty


class BlockRecord(object):
    """
    TODO
    """
    @abstractproperty
    def locator(self):
        """
        Gets the locator of the block to which this record refers.
        :return: the block record
        :rtype: str
        """

    @abstractproperty
    def timestamp(self):
        """
        Gets the timestamp of this record.
        :return: the timestamp
        :rtype: datetime
        """


class BlockPutRecord(BlockRecord):
    """
    TODO
    """
    @abstractproperty
    def size(self):
        """
        Gets the size of the block that was put.
        :return: the size in bytes
        :rtype: int
        """


class BlockGetRecord(BlockRecord):
    """
    TODO
    """


class BlockDeleteRecord(BlockRecord):
    """
    TODO
    """
