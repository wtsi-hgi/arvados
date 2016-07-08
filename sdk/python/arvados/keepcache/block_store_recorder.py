from abc import ABCMeta
from datetime import datetime

from sqlalchemy import Column, String, Integer, create_engine, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from arvados.keepcache.models import BlockRecord, BlockPutRecord, \
    BlockGetRecord, BlockDeleteRecord


class BlockStoreUsageRecorder(object):
    """
    Recorder for the usage of a block store.
    """
    __metaclass__ = ABCMeta

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
        given locator.
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

    class _BlockRecord(SQLAlchemyModel, BlockRecord):
        __abstract__ = True
        __tablename__ = BlockRecord.__name__
        locator = Column(String, primary_key=True)
        timestamp = Column(DateTime)

    class _BlockPutRecord(_BlockRecord, BlockPutRecord):
        __tablename__ = BlockPutRecord.__name__
        size = Column(Integer)

    class _BlockGetRecord(_BlockRecord, BlockGetRecord):
        __tablename__ = BlockGetRecord.__name__

    class _BlockDeleteRecord(_BlockRecord, BlockDeleteRecord):
        __tablename__ = BlockDeleteRecord.__name__

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
            DatabaseBlockStoreUsageRecorder._BlockGetRecord, locator)
        self._store(record)

    def record_put(self, locator, content_size):
        record = self._create_record(
            DatabaseBlockStoreUsageRecorder._BlockPutRecord, locator)
        record.size = content_size
        self._store(record)

    def record_delete(self, locator):
        record = self._create_record(
            DatabaseBlockStoreUsageRecorder._BlockDeleteRecord, locator)
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
        Stores the given record.
        :param record: the record to store
        :type record: Record
        """
        self._database.add(record)
        self._database.commit()
