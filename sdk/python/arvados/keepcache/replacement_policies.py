from abc import ABCMeta, abstractmethod

from datetime import datetime


class CacheReplacementPolicy(object):
    """
    Cache replacement policy.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def next_to_delete(self, block_store_usage_recorder):
        """
        Gets the locator of the next block that is to be deleted.

        Return `None` if no blocks can be removed.
        :param block_store_usage_recorder:
        :type block_store_usage_recorder: BlockStoreBookkeeper
        :return:
        :rtype: Optional[str]
        """


class FIFOCacheReplacementPolicy(CacheReplacementPolicy):
    """
    Cache replacement policy where the first block in is the first block out.
    """
    def next_to_delete(self, block_store_usage_recorder):
        active_block_put_records = block_store_usage_recorder.get_active()
        if len(active_block_put_records) == 0:
            return None
        oldest = (datetime.max, None)   # type: Tuple[datetime, Optional[str]]
        for record in active_block_put_records:
            if record.timestamp < oldest[0]:
                oldest = (record.timestamp, record.locator)
        return str(oldest[1])
