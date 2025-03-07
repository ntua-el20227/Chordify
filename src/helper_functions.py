import hashlib
import time
import subprocess
import requests
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


def replica_handler(node):
    time.sleep(1)
    if node.data_store is not None:
        node.generate_replicas(node.data_store)

    if node.successor["node_id"] != node.node_id:
        # Fix my successor's replicas
        url_node_info = f"http://{node.successor['ip']}:{node.successor['port']}/node_info"
        resp_info = requests.get(url_node_info).json()
        node_info = resp_info
        data_succ = node_info.get("data_store")
        if data_succ is not None:
            url_gen_pred = f"http://{node.successor['ip']}:{node.successor['port']}/generate_replicas"
            requests.post(url_gen_pred, json={"keys": data_succ})