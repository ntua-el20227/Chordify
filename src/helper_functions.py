import hashlib

def in_interval(x, start, end):
    """
    Check if x is in the circular interval (start, end] modulo 2^16.
    Assumes the values are already reduced modulo 2^16.
    """
    if start < end:
        return start < x <= end
    return x > start or x <= end


def hash_function(key):
    """Compute SHA-1 hash of a key mod 2^16."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 16)
