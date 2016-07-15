from abc import ABCMeta, abstractmethod

from datetime import datetime


class CacheReplacementPolicy(object):
    """
    Cache replacement policy.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def next_to_delete(self, bookkeeper):
        """
        Gets the locator of the next block that is to be deleted.

        Return `None` if no blocks can be removed.
        :param bookkeeper:
        :type bookkeeper: BlockStoreBookkeeper
        :return:
        :rtype: Optional[str]
        """


class FIFOCacheReplacementPolicy(CacheReplacementPolicy):
    """
    Cache replacement policy where the first block in is the first block out.
    """
    def next_to_delete(self, bookkeeper):
        active_put_records = bookkeeper.get_active()
        if len(active_put_records) == 0:
            return None
        oldest = (datetime.max, None)   # type: Tuple[datetime, Optional[str]]
        for record in active_put_records:
            if record.timestamp < oldest[0]:
                oldest = (record.timestamp, record.locator)
        return str(oldest[1])


class LastUsedReplacementPolicy(CacheReplacementPolicy):
    """
    Cache replacement policy where the block accessed the longest time ago is
    removed first.
    """
    def next_to_delete(self, bookkeeper):
        active_put_records = bookkeeper.get_active()
        if len(active_put_records) == 0:
            return None

        oldest_timestamp = min(
            active_put_records, key=lambda record: record.timestamp).timestamp
        get_records = bookkeeper.get_all_get_records(
            locators=[record.locator for record in active_put_records],
            since=oldest_timestamp)

        records = dict()    # type: Dict[str, Record]
        for record in active_put_records | get_records:
            locator = record.locator
            if record.locator not in records \
                    or record.timestamp > records[locator].timestamp:
                records[locator] = record
        return min(records.values(), key=lambda record: record.timestamp).locator




