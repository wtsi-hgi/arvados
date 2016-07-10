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


class DatabaseBlockStoreUsageRecorder(BlockStoreUsageRecorder):
    """
    Recorder for usage of a block store where records are kept in a database.
    """
    SQLAlchemyModel = declarative_base()

    class _SqlAlchemyBlockRecord(SQLAlchemyModel, BlockRecord):
        __abstract__ = True
        __tablename__ = BlockRecord.__name__
        locator = Column(String, primary_key=True)
        timestamp = Column(DateTime)

    class _SqlAlchemyBlockPutRecord(_SqlAlchemyBlockRecord, BlockPutRecord):
        __tablename__ = BlockPutRecord.__name__
        size = Column(Integer)

    class _SqlAlchemyBlockGetRecord(_SqlAlchemyBlockRecord, BlockGetRecord):
        __tablename__ = BlockGetRecord.__name__

    class _SqlAlchemyBlockDeleteRecord(_SqlAlchemyBlockRecord, BlockDeleteRecord):
        __tablename__ = BlockDeleteRecord.__name__

    @staticmethod
    def _create_record(cls, locator):
        """
        TODO
        :param cls:
        :type cls: type
        :param locator:
        :type locator: str
        :return:
        :rtype: Record
        """
        record = cls()
        record.locator = locator
        record.timestamp = datetime.now()
        return record

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
        """
        SELECT size
        FROM BlockPutRecord Put
        LEFT JOIN BlockDeleteRecord Delete
        ON Put.locator == Delete.locator
        WHERE 
        """
        Put = DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockPutRecord
        Delete = DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockDeleteRecord
        self._database.query(Put, Delete).\
            filter(Put.locator == Delete.locator).\
            filter(Put.timestamp > Delete.timestamp)

    def record_get(self, locator):
        record = DatabaseBlockStoreUsageRecorder._create_record(
            DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockGetRecord, locator)
        self._store(record)

    def record_put(self, locator, content_size):
        record = DatabaseBlockStoreUsageRecorder._create_record(
            DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockPutRecord, locator)
        record.size = content_size
        self._store(record)

    def record_delete(self, locator):
        record = DatabaseBlockStoreUsageRecorder._create_record(
            DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockDeleteRecord, locator)
        self._store(record)

    def _store(self, record):
        """
        Stores the given record.
        :param record: the record to store
        :type record: Record
        """
        self._database.add(record)
        self._database.commit()
