from abc import abstractproperty, ABCMeta

from sqlalchemy import Column, DateTime, Integer, String, TypeDecorator
from sqlalchemy.ext.declarative import declarative_base


class BlockRecord(object):
    """
    Timestamped record of an interaction involving a block that has a locator.
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


class BlockGetRecord(BlockRecord):
    """
    Timestamped record of a block being accessed from a cache (implies a cache
    hit).
    """


class BlockModificationRecord(BlockRecord):
    """
    Timestamped record of a modification to a block that has a locator.
    """


class BlockPutRecord(BlockModificationRecord):
    """
    Timestamped record of a block of a certain size being put into a cache
    (implies a cache miss).
    """
    @abstractproperty
    def size(self):
        """
        Gets the size of the block that was put.
        :return: the size in bytes
        :rtype: int
        """


class BlockDeleteRecord(BlockModificationRecord):
    """
    Timestamped record of a block being deleted from a cache.
    """


class InMemoryBlockRecord(BlockRecord):
    """
    In memory block record.
    """
    __metaclass__ = ABCMeta

    def __init__(self, locator, timestamp):
        self._locator = locator
        self._timestamp = timestamp

    @property
    def locator(self):
        return self._locator

    @property
    def timestamp(self):
        return self._timestamp


class InMemoryBlockPutRecord(InMemoryBlockRecord, BlockPutRecord):
    """
    In memory block put record.
    """
    def __init__(self, locator, timestamp, size):
        super(InMemoryBlockPutRecord, self). \
            __init__(locator, timestamp)
        self._size = size

    @property
    def size(self):
        return self._size


class InMemoryBlockGetRecord(InMemoryBlockRecord, BlockGetRecord):
    """
    In memory block get record.
    """


class InMemoryBlockDeleteRecord(InMemoryBlockRecord, BlockDeleteRecord):
    """
    In memory block delete record.
    """


SqlAlchemyModel = declarative_base()


class SqlAlchemyBlockRecord(SqlAlchemyModel, BlockRecord):
    """
    SQLAlchemy block record.
    """
    class _Python2String(TypeDecorator):
        impl = String

        def process_result_value(self, value, dialect):
            if isinstance(value, unicode):
                value = value.encode("utf-8")
                assert isinstance(value, str)
            return value

    __abstract__ = True
    __tablename__ = BlockRecord.__name__
    id = Column(Integer, primary_key=True)
    locator = Column(_Python2String)
    timestamp = Column(DateTime)


class SqlAlchemyBlockPutRecord(SqlAlchemyBlockRecord, BlockPutRecord):
    """
    SQLAlchemy block put record.
    """
    __tablename__ = BlockPutRecord.__name__
    size = Column(Integer)


class SqlAlchemyBlockGetRecord(SqlAlchemyBlockRecord, BlockGetRecord):
    """
    SQLAlchemy block get record.
    """
    __tablename__ = BlockGetRecord.__name__


class SqlAlchemyBlockDeleteRecord(SqlAlchemyBlockRecord, BlockDeleteRecord):
    """
    SQLAlchemy block delete record.
    """
    __tablename__ = BlockDeleteRecord.__name__
