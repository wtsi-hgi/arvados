import bisect
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import defaultdict
from datetime import datetime

from sqlalchemy import Column, String, Integer, create_engine, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.sql.elements import or_


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


class BlockPutRecord(BlockRecord):
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


class BlockGetRecord(BlockRecord):
    """
    Timestamped record of a block being accessed from a cache (implies a cache
    hit).
    """


class BlockDeleteRecord(BlockRecord):
    """
    Timestamped record of a block being delted from a cache.
    """


class BlockStoreUsageRecorder(object):
    """
    Bookkeeper for the usage of a block store.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_active(self):
        """
        Gets the put records of the blocks that are currently in the block store
        whose usage is being recorded.
        :return: active blocks
        :rtype: Set[BlockPutRecord]
        """

    @abstractmethod
    def get_all_records(self):
        """
        Gets all of the recorded events.
        :return: records of the events
        :rtype: Set[Record]
        """

    @abstractmethod
    def record_get(self, locator):
        """
        Records an access to the block in the store with the given locator.
        :param locator: the block identifier
        """

    @abstractmethod
    def record_put(self, locator, content_size):
        """
        Records the writing of content of the given size as the block with the
        given locator.
        :param locator: the block identifier
        :type locator: str
        :param content_size: the size of the block content
        :type content_size: int
        """

    @abstractmethod
    def record_delete(self, locator):
        """
        Records the deletion of the block with the given locator.
        :param locator: the block identifier
        :type locator: str
        """

    def get_size(self):
        """
        Gets the current (known) size of the block store.
        :return: the current size of the block store in bytes
        :rtype: int
        """
        return sum([put.size for put in self.get_active()])


class InMemoryBlockStoreUsageRecorder(BlockStoreUsageRecorder):
    """
    In memory bookkeeper for usage of a block store.
    """
    class InMemoryBlockRecord(BlockRecord):
        """In memory block record."""
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
        """In memory block put record."""
        def __init__(self, locator, timestamp, size):
            super(InMemoryBlockStoreUsageRecorder.InMemoryBlockPutRecord, self).__init__(
                locator, timestamp)
            self._size = size

        @property
        def size(self):
            return self._size

    class InMemoryBlockGetRecord(InMemoryBlockRecord, BlockGetRecord):
        """In memory block get record."""

    class InMemoryBlockDeleteRecord(InMemoryBlockRecord, BlockDeleteRecord):
        """In memory block delete record."""

    def __init__(self):
        self._records = defaultdict(set)  # type: Dict[type, Set[Record]]

    def get_active(self):
        PutRecord = InMemoryBlockStoreUsageRecorder.InMemoryBlockPutRecord
        puts = self._records[PutRecord]
        deletes = self._records[InMemoryBlockStoreUsageRecorder.InMemoryBlockDeleteRecord]
        locator_records = dict()  # type: Dict[str, Record]

        for record in puts | deletes:
            if record.locator not in locator_records:
                locator_records[record.locator] = record
            else:
                if record.timestamp > locator_records[record.locator].timestamp:
                    locator_records[record.locator] = record

        return [record for record in locator_records.values() if isinstance(record, PutRecord)]

    def get_all_records(self):
        records = set()     # type: Set[Record]
        for record_type in self._records.keys():
            records = records.union(self._records[record_type])
        return records

    def record_get(self, locator):
        GetRecord = InMemoryBlockStoreUsageRecorder.InMemoryBlockGetRecord
        record = GetRecord(locator, datetime.now())
        self._records[GetRecord].add(GetRecord(locator, datetime.now()))

    def record_put(self, locator, content_size):
        PutRecord = InMemoryBlockStoreUsageRecorder.InMemoryBlockPutRecord
        self._records[PutRecord].add(PutRecord(locator, datetime.now(), content_size))

    def record_delete(self, locator):
        DeleteRecord = InMemoryBlockStoreUsageRecorder.InMemoryBlockDeleteRecord
        self._records[DeleteRecord].add(DeleteRecord(locator, datetime.now()))


class DatabaseBlockStoreUsageRecorder(BlockStoreUsageRecorder):
    """
    Recorder for usage of a block store where records are kept in a database.
    """
    SQLAlchemyModel = declarative_base()

    class _SqlAlchemyBlockRecord(SQLAlchemyModel, BlockRecord):
        __abstract__ = True
        __tablename__ = BlockRecord.__name__
        id = Column(Integer, primary_key=True)
        locator = Column(String)
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
        Creates a record of the given class type with the given locator and a
        timestamp of now.
        :param cls: the type of record (must be a subtype of `Record`)
        :type cls: type
        :param locator: the record's locator
        :type locator: str
        :return: the created record
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
        self._engine = create_engine(database_location)
        DatabaseBlockStoreUsageRecorder.SQLAlchemyModel.metadata.create_all(
            bind=self._engine)

    def get_active(self):
        """
        TODO
        :return:
        """
        Put = DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockPutRecord
        Delete = DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockDeleteRecord
        session = self._create_session()

        subquery = session.query(Put, func.max(Delete.timestamp).label(
            "latest_delete")). \
            join(Delete, Put.locator == Delete.locator). \
            group_by(Put.locator). \
            subquery()

        results = session.query(Put). \
            outerjoin(subquery, subquery.c.locator == Put.locator). \
            filter(or_(
            subquery.c.latest_delete == None,
            Put.timestamp > subquery.c.latest_delete
        )).all()
        session.close()
        return set(results)

    def get_all_records(self):
        session = self._create_session()
        record_types = [
            DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockGetRecord,
            DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockPutRecord,
            DatabaseBlockStoreUsageRecorder._SqlAlchemyBlockDeleteRecord
        ]
        all_records = set()     # type: Set[Record]
        for record_type in record_types:
            records = session.query(record_type).all()
            all_records = all_records.union(records)
        return all_records

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

    def _create_session(self):
        Session = sessionmaker(bind=self._engine)
        return Session()

    def _store(self, record):
        """
        Stores the given record.
        :param record: the record to store
        :type record: Record
        """
        session = self._create_session()
        session.add(record)
        session.commit()
        session.close()
