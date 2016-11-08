import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime

import lmdb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.sql.elements import or_

from arvados.keepcache._common import to_bytes, datetime_to_unix_time, \
    unix_time_to_datetime, ONE_MB
from arvados.keepcache._locks import GlobalLock
from arvados.keepcache.block_store_records import BlockGetRecord, \
    BlockPutRecord, BlockDeleteRecord, InMemoryBlockDeleteRecord, \
    InMemoryBlockPutRecord, InMemoryBlockGetRecord, SqlAlchemyBlockDeleteRecord, \
    SqlAlchemyBlockPutRecord, SqlAlchemyBlockGetRecord, SqlAlchemyModel

_logger = logging.getLogger(__name__)


class BlockStoreBookkeeper(object):
    """
    Bookkeeper for the usage of a block store.

    Concrete methods can be overridden if there is a more efficient way of
    achieving the functionality in the underlying technology.
    """
    __metaclass__ = ABCMeta

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

    def get_active(self):
        """
        Gets the put records of the blocks that are currently in the block
        store whose usage is being recorded.
        :return: active blocks
        :rtype: Set[BlockPutRecord]
        """
        locator_records = dict()  # type: Dict[str, Record]
        for record in self.get_all_put_records() \
                | self.get_all_delete_records():
            if record.locator not in locator_records:
                locator_records[record.locator] = record
            else:
                if record.timestamp > locator_records[record.locator].timestamp:
                    locator_records[record.locator] = record

        return {record for record in locator_records.values()
                if isinstance(record, BlockPutRecord)}

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

    def _get_current_timestamp(self):
        """
        Gets a timestamp for the current time.
        :return: the current time
        :rtype: datetime
        """
        return datetime.utcnow()


class InMemoryBlockStoreBookkeeper(BlockStoreBookkeeper):
    """
    In memory bookkeeper for usage of a block store.
    """
    def __init__(self):
        self._records = defaultdict(set)  # type: Dict[type, Set[Record]]

    def record_get(self, locator):
        record = InMemoryBlockGetRecord(
            locator, self._get_current_timestamp()
        )
        self._records[BlockGetRecord].add(record)

    def record_put(self, locator, content_size):
        record = InMemoryBlockPutRecord(
            locator, self._get_current_timestamp(), content_size
        )
        self._records[BlockPutRecord].add(record)

    def record_delete(self, locator):
        record = InMemoryBlockDeleteRecord(
            locator, self._get_current_timestamp()
        )
        self._records[BlockDeleteRecord].add(record)

    def _get_all_records_of_type(self, record_type, locators, since):
        # Note: copy of set used to prevent changes during use
        records = set(self._records[record_type])
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
    _RECORD_TO_SQL_RECORD = {
        BlockGetRecord: SqlAlchemyBlockGetRecord,
        BlockPutRecord: SqlAlchemyBlockPutRecord,
        BlockDeleteRecord: SqlAlchemyBlockDeleteRecord
    }

    def __init__(self, database_location, write_lock_location=None):
        """
        Constructor.
        :param database_location: the location of the database
        :type database_location: str
        :param write_lock_location: write lock file. This is required for
        SQLite as it does not support concurrent writes
        :type write_lock_location: Optional[str]
        """
        self._engine = create_engine(database_location)
        SqlAlchemyModel.metadata.create_all(bind=self._engine)
        self._write_lock = GlobalLock(write_lock_location) if write_lock_location else None

    def record_get(self, locator):
        record = self._create_record(SqlAlchemyBlockGetRecord, locator)
        self._store(record)

    def record_put(self, locator, content_size):
        record = self._create_record(SqlAlchemyBlockPutRecord, locator)
        record.size = content_size
        self._store(record)

    def record_delete(self, locator):
        record = self._create_record(SqlAlchemyBlockDeleteRecord, locator)
        self._store(record)

    def get_active(self):
        PutRecord = SqlAlchemyBlockPutRecord
        DeleteRecord = SqlAlchemyBlockDeleteRecord
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

    def _get_all_records_of_type(self, record_type, locators, since):
        sql_record_type = SqlBlockStoreBookkeeper._RECORD_TO_SQL_RECORD[record_type]
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
        record.timestamp = self._get_current_timestamp()
        return record

    def _create_session(self):
        """
        Creates SQLAlchemy session.
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
        if self._write_lock is not None:
            self._write_lock.acquire()
        session = self._create_session()
        session.add(record)
        session.commit()
        session.close()
        if self._write_lock is not None:
            self._write_lock.release()


class LMDBBlockStoreBookkeeper(BlockStoreBookkeeper):
    """
    LMDB backed block store bookkeeper.
    """
    _PUT_DATABASE_NAME = "Put"
    _GET_DATABASE_NAME = "Get"
    _DELETE_DATABASE_NAME = "Delete"
    _SIZE_DATABASE = "Size"

    _RECORD_TO_IN_MEMORY_RECORD = {
        BlockGetRecord: InMemoryBlockGetRecord,
        BlockPutRecord: InMemoryBlockPutRecord,
        BlockDeleteRecord: InMemoryBlockDeleteRecord
    }

    def __init__(self, lmdb_directory):
        """
        Constructor.
        :param lmdb_directory: the directory to be used with LMDB.
        :type lmdb_directory: str
        """
        self._environment = lmdb.open(lmdb_directory, max_dbs=4, map_size=100 * ONE_MB)
        self._put_database = self._environment.open_db(
            LMDBBlockStoreBookkeeper._PUT_DATABASE_NAME, dupsort=True)
        self._get_database = self._environment.open_db(
            LMDBBlockStoreBookkeeper._GET_DATABASE_NAME, dupsort=True)
        self._delete_database = self._environment.open_db(
            LMDBBlockStoreBookkeeper._DELETE_DATABASE_NAME, dupsort=True)
        self._size_database = self._environment.open_db(
            LMDBBlockStoreBookkeeper._SIZE_DATABASE, dupsort=True)
        self._type_database_mapping = {
            BlockGetRecord: self._get_database,
            BlockPutRecord: self._put_database,
            BlockDeleteRecord: self._delete_database
        }

    def record_get(self, locator):
        with self._environment.begin(write=True, db=self._get_database) as transaction:
            transaction.put(to_bytes(locator), self._get_database_insertable_timestamp())

    def record_put(self, locator, content_size):
        locator = to_bytes(locator)
        with self._environment.begin(write=True) as transaction:
            transaction.put(locator, self._get_database_insertable_timestamp(), db=self._put_database)
            transaction.put(locator, to_bytes(content_size), db=self._size_database)

    def record_delete(self, locator):
        with self._environment.begin(write=True, db=self._delete_database) as transaction:
            transaction.put(to_bytes(locator), self._get_database_insertable_timestamp())

    def _get_all_records_of_type(self, record_type, locators, since):
        database = self._type_database_mapping[record_type]
        record_type = LMDBBlockStoreBookkeeper._RECORD_TO_IN_MEMORY_RECORD[record_type]
        records = set()     # type: Set[Record]

        with self._environment.begin() as transaction:
            with transaction.cursor(db=database) as cursor:
                for _, locator in enumerate(cursor.iternext(values=False)):
                    # XXX: If filter, could iterate on locators instead
                    if locators is None or locator in locators:
                        for _, timestamp in enumerate(cursor.iterprev_dup(keys=False)):
                            timestamp = unix_time_to_datetime(timestamp)
                            if since is None or timestamp >= since:
                                if not issubclass(record_type, BlockPutRecord):
                                    record = record_type(locator, timestamp)
                                else:
                                    size = int(transaction.get(to_bytes(locator), db=self._size_database))
                                    record = record_type(locator, timestamp, size)
                                records.add(record)
                        # Move cursor back to continue iteration through keys
                        cursor.last_dup()
        return records

    def _get_database_insertable_timestamp(self):
        """
        Gets the current timestamp in a form that can be inserted into the
        database.
        :return: the current timestamp, storable in LMDB
        :rtype: bytearray
        """
        return to_bytes(datetime_to_unix_time(self._get_current_timestamp()))
