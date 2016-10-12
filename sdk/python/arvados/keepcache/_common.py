

def to_bytes(str_or_bytes):
    """
    TODO
    :param str_or_bytes:
    :type str_or_bytes: Union[str, bytes]
    :return:
    :rtype: bytes
    """
    return str_or_bytes if isinstance(str_or_bytes, bytes) \
        else str_or_bytes.encode()
