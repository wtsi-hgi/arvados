

def to_bytes(str_or_bytes):
    """
    Converts the given string or bytes to bytes (no-op with the latter).
    :param str_or_bytes: the string or bytes
    :type str_or_bytes: Union[str, bytes]
    :return: bytes representation
    :rtype: bytes
    """
    return str_or_bytes if isinstance(str_or_bytes, bytes) \
        else str_or_bytes.encode()
