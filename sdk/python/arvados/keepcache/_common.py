from datetime import datetime

ONE_MB = 1 * 1024 * 1024
ONE_GB = 1 * ONE_MB * 1024


def to_bytes(value):
    """
    Converts the given value to bytes, based on the value's string
    representation (no-op if already bytes).
    :param value: the value to convert
    :type value: Any
    :return: bytes representation
    :rtype: bytes
    """
    return value if isinstance(value, bytes) \
        else str(value).encode()


def datetime_to_unix_time(timestamp):
    """
    TODO
    :param timestamp: timestamp to convert (must be in UTC)
    :type timestamp: datetime
    :return:
    """
    return (timestamp - datetime.utcfromtimestamp(0)).total_seconds()


def unix_time_to_datetime(unix_time):
    """
    TODO
    :param unix_time:
    :return:
    """
    unix_time = float(unix_time)
    return datetime.utcfromtimestamp(unix_time)
