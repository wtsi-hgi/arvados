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


class BlockStoreBookkeeper(object):
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

    @abstractmethod
    def _get_all_records_of_type(self, record_type, locators, since):
        """
        Gets all records of the given type, with optional filters.
        :param record_type: the type of the record
        :type record_type: type
        :param locators: optional locators to filter on
        :type locators: Optional[Set[str]]
        :param since: optionally filter out all records older than this value
        :type since: Optional[datetime]
        :return: the records
        :rtype: Set[Record]
        """

    def get_active_storage_size(self):
        """
        Gets the current size of entries that are active in the block store.
        :return: the current size of the block store in bytes
        :rtype: int
        """
        return sum([put.size for put in self.get_active()])

    def get_all_get_records(self, locators=None, since=None):
        """
        Gets all get records known by this bookkeeper, with optional filters.
        :param locators: optional locators to limit by
        :type locators: Optional[Set[str]]
        :param since: optionally filter out all records older than this value
        :type since: Optional[datetime]
        :return: the get records
        :rtype: Set[BlockGetRecord]
        """
        return self._get_all_records_of_type(BlockGetRecord, locators, since)

    def get_all_put_records(self, locators=None, since=None):
        """
        Gets all put records known by this bookkeeper, with optional filters.
        :param locators: optional locators to limit by
        :type locators: Optional[Set[str]]
        :param since: optionally filter out all records older than this value
        :type since: Optional[datetime]
        :return: the put records
        :rtype: Set[BlockPutRecord]
        """
        return self._get_all_records_of_type(BlockPutRecord, locators, since)

    def get_all_delete_records(self, locators=None, since=None):
        """
        Gets all delete records known by this bookkeeper, with optional filters.
        :param locators: optional locators to limit by
        :type locators: Optional[Set[str]]
        :param since: optionally filter out all records older than this value
        :type since: Optional[datetime]
        :return: the delete records
        :rtype: Set[BlockDeleteRecord]
        """
        return self._get_all_records_of_type(BlockDeleteRecord, locators, since)

    def get_all_records(self, locators=None, since=None):
        """
        Gets all of the recorded events with optional filters.
        :param locators: optional locators to limit by
        :type locators: Optional[Set[str]]
        :param since: optionally filter out all records older than this value
        :type since: Optional[datetime]
        :return: records of the events
        :rtype: Set[Record]
        """
        # This implementation is naive: it can be overriden if there is a more
        # optimal method of getting all records in the implemented bookkeeper
        return self.get_all_get_records(locators, since) \
               | self.get_all_put_records(locators, since) \
               | self.get_all_delete_records(locators, since)

    def get_current_timestamp(self):
        """
        Gets a timestamp for the current time.
        :return: the current time
        :rtype: datetime
        """
        return datetime.now()


class InMemoryBlockStoreBookkeeper(BlockStoreBookkeeper):
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
            super(InMemoryBlockStoreBookkeeper.InMemoryBlockPutRecord, self).__init__(
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
        locator_records = dict()  # type: Dict[str, Record]

        for record in self._records[BlockPutRecord] \
                | self._records[BlockDeleteRecord]:
            if record.locator not in locator_records:
                locator_records[record.locator] = record
            else:
                if record.timestamp > locator_records[record.locator].timestamp:
                    locator_records[record.locator] = record

        return {record for record in locator_records.values()
                if isinstance(record, BlockPutRecord)}

    def record_get(self, locator):
        record = InMemoryBlockStoreBookkeeper.InMemoryBlockGetRecord(
            locator, self.get_current_timestamp()
        )
        self._records[BlockGetRecord].add(record)

    def record_put(self, locator, content_size):
        record = InMemoryBlockStoreBookkeeper.InMemoryBlockPutRecord(
            locator, self.get_current_timestamp(), content_size
        )
        self._records[BlockPutRecord].add(record)

    def record_delete(self, locator):
        record = InMemoryBlockStoreBookkeeper.InMemoryBlockDeleteRecord(
            locator, self.get_current_timestamp()
        )
        self._records[BlockDeleteRecord].add(record)

    def _get_all_records_of_type(self, record_type, locators, since):
        records = self._records[record_type]
        if locators is not None:
            records = {record for record in records if record.locator in locators}
        if since is not None:
            records = {record for record in records if record.timestamp >= since}
        return records


class SqlBlockStoreBookkeeper(BlockStoreBookkeeper):
    """
    Bookkeeper of usage of a block store where records are kept in an SQL
    database.
    """
    _SQLAlchemyModel = declarative_base()

    class _SqlAlchemyBlockRecord(_SQLAlchemyModel, BlockRecord):
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

    SQL_ALCHEMY_RECORD_TYPES = [
        _SqlAlchemyBlockGetRecord,
        _SqlAlchemyBlockPutRecord,
        _SqlAlchemyBlockDeleteRecord
    ]

    @staticmethod
    def _get_sql_record_type(generic_type):
        """
        Gets the SQL record type that is a subclass of the generic block record
        type given.
        :param generic_type: the generic record type
        :type generic_type: type
        :return: the SQL record type else `None` if none found
        :rtype: Optional[type]
        """
        assert issubclass(generic_type, BlockRecord)
        for record_type in SqlBlockStoreBookkeeper.SQL_ALCHEMY_RECORD_TYPES:
            assert issubclass(record_type, SqlBlockStoreBookkeeper._SQLAlchemyModel)
            if issubclass(record_type, generic_type):
                return record_type
        return None

    def __init__(self, database_location):
        """
        Constructor.
        :param database_location: the location of the database
        :type database_location: str
        """
        self._engine = create_engine(database_location)
        SqlBlockStoreBookkeeper._SQLAlchemyModel.metadata.create_all(
            bind=self._engine)

    def get_active(self):
        PutRecord = SqlBlockStoreBookkeeper._SqlAlchemyBlockPutRecord
        DeleteRecord = SqlBlockStoreBookkeeper._SqlAlchemyBlockDeleteRecord
        session = self._create_session()
        subquery = session.query(PutRecord, func.max(DeleteRecord.timestamp).label(
            "latest_delete")). \
            join(DeleteRecord, PutRecord.locator == DeleteRecord.locator). \
            group_by(PutRecord.locator). \
            subquery()

        query = session.query(PutRecord). \
            outerjoin(subquery, subquery.c.locator == PutRecord.locator). \
            filter(or_(
            subquery.c.latest_delete == None,
            PutRecord.timestamp > subquery.c.latest_delete
        ))
        results = query.all()
        session.close()
        return set(results)

    def record_get(self, locator):
        record = self._create_record(
            SqlBlockStoreBookkeeper._SqlAlchemyBlockGetRecord, locator)
        self._store(record)

    def record_put(self, locator, content_size):
        record = self._create_record(
            SqlBlockStoreBookkeeper._SqlAlchemyBlockPutRecord, locator)
        record.size = content_size
        self._store(record)

    def record_delete(self, locator):
        record = self._create_record(
            SqlBlockStoreBookkeeper._SqlAlchemyBlockDeleteRecord, locator)
        self._store(record)

    def _get_all_records_of_type(self, record_type, locators, since):
        sql_record_type = SqlBlockStoreBookkeeper._get_sql_record_type(record_type)
        session = self._create_session()
        query = session.query(sql_record_type)
        if locators is not None:
            query = query.filter(sql_record_type.locator.in_(locators))
        if since is not None:
            query = query.filter(sql_record_type.timestamp >= since)
        records = query.all()
        session.close()
        return set(records)

    def _create_record(self, cls, locator):
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
        record.timestamp = self.get_current_timestamp()
        return record

    def _create_session(self):
        """
        Creates SQLAlchmeny session.
        :return: SQLAlchemy session
        """
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
