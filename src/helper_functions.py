import hashlib
import time
import os

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

def shutdown_server():
    # Shut down the server using os._exit() to avoid the SystemExit exception
    time.sleep(1)
    os._exit(0)

