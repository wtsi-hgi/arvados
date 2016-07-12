from abc import ABCMeta, abstractmethod

from datetime import datetime


class CacheReplacementPolicy(object):
    """
    TODO
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def next_to_delete(self, block_put_records):
        """
        TODO

        ? if no block records.
        :param block_put_records:
        :type block_put_records: Iterable[PutRecord]
        :return:
        """


class FIFOCacheReplacementPolicy(CacheReplacementPolicy):
    """
    Replacement policy where the first block in is the first block out.
    """
    def next_to_delete(self, block_put_records):
        if len(block_put_records) == 0:
            raise ValueError("No block put records given")
        oldest = (datetime.max, None)   # type: Tuple[datetime, Optional[str]]
        for record in block_put_records:
            if record.timestamp < oldest[0]:
                oldest = (record.timestamp, record.locator)
        return str(oldest[1])
