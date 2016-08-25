import logging
import threading
from abc import ABCMeta
from threading import Lock

_logger = logging.getLogger(__name__)


class CacheSlot(object):
    """
    Model of a slot in the cache.
    """
    __metaclass__ = ABCMeta

    def __init__(self, locator, content=None):
        """
        Constructor.
        :param locator: identifier
        :type locator: str
        :param content: optional content already known
        :type content: Optional[bytearray]
        """
        self.locator = locator
        self.ready = threading.Event()
        self._content = content

    @property
    def content(self):
        """
        Gets the contents of the cache slot.
        :return: the contents of the cache slot or `None` if not set
        """
        return self._content

    def get(self):
        """
        Gets this cache slot's contents. If the contents are not set, it will
        block until they are.
        :return: the contents
        :rtype bytearray
        """
        _logger.debug("Getting content for cache slot associated to `%s` "
                      "(ready=%s)" % (self.locator, self.ready.is_set()))
        self.ready.wait()
        return self.content

    def set(self, content):
        """
        Sets this cache slot's contents to the given value and marks slot as
        ready.
        :param content: the value to set the contents to
        :rtype: bytearray
        """
        self._content = content
        _logger.debug("Set content for cache slot associated to `%s`"
                      % self.locator)
        self.ready.set()

    def size(self):
        """
        The size of this slot's contents. Will return 0 if contents is `None`.
        :return: the size of the contents
        :rtype: int
        """
        if self.content is None:
            return 0
        else:
            return len(self.content)


class GetterSetterCacheSlot(CacheSlot):
    """
    Model of a slot in the cache where the contents of the slot are loaded and
    set using the given getter and setter methods.
    """
    def __init__(self, locator, content_getter, content_setter, content=None):
        """
        Constructor.
        :param locator: identifier
        :type locator: str
        :param content_getter: method that gets the contents associated to the
        given locator or `None` if the contents have not been defined
        :type content_getter: Callable[[str], Optional[bytearray]]
        :param content_setter: method that sets the contents associated to the
        given locator
        :type content_setter: Callable[[str, bytearray], None]
        :param content: optional content already known
        :type content: Optional[bytearray]
        """
        super(GetterSetterCacheSlot, self).__init__(locator, content)
        self._content_getter = content_getter
        self._content_setter = content_setter
        self._set_lock = Lock()
        self._get_lock = Lock()

    def get(self):
        _logger.debug("Getting content for cache slot associated to `%s` "
                      % self.locator)
        if self.content is not None:
            return self.content
        else:
            with self._get_lock:
                if self.content is not None:
                    # Another thread has got whilst waiting for get lock
                    return self.content
                else:
                    content = self._content_getter(self.locator)
                    if content is not None:
                        super(GetterSetterCacheSlot, self).set(content)
                        return content
                    else:
                        return super(GetterSetterCacheSlot, self).get()

    def set(self, content):
        with self._set_lock:
            _logger.debug("Setting content of %d byte(s) for `%s`"
                          % (len(content), self.locator))
            self._content_setter(self.locator, content)
            super(GetterSetterCacheSlot, self).set(content)
