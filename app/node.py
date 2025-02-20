import hashlib


class Node:
    def __init__(self, ip="127.0.0.1", port=5000, replication_factor=1):
        self.ip = ip
        self.port = port
        self.id = self.generate_id(ip, port)
        self.data = {}  # Stores <hashed_key, value> pairs for which this node is responsible

        # Pointers for Chord routing:
        # In a simplified version, each node keeps a reference to its successor and predecessor.
        self.predecessor = None
        self.successor = self  # Initially points to itself until more nodes join

        # Replication factor for data replication (default is 1 = no replication)
        self.replication_factor = replication_factor

    def generate_id(self, ip, port):
        """
        Generate a unique ID for the node using SHA1 on the string "ip:port".
        """
        key = f"{ip}:{port}".encode()
        return hashlib.sha1(key).hexdigest()

    def insert(self, key, value):
        """
        Insert a new <key, value> pair or update an existing one.
        The key is first hashed to find the correct position in the ring.
        If the key exists, update the value by concatenation.
        Note: In a complete implementation, the request should be forwarded
        to the node responsible for the hashed key if this node isn't it.
        """
        hashed_key = hashlib.sha1(key.encode()).hexdigest()
        if hashed_key in self.data:
            # Concatenate new value to the existing one
            self.data[hashed_key] += f",{value}"
        else:
            self.data[hashed_key] = value

        # TODO: Implement replication and proper forwarding in a multi-node setup.
        return {
            "status": "inserted",
            "node_id": self.id,
            "key": key,
            "value": self.data[hashed_key]
        }

    def query(self, key):
        """
        Query a key in the DHT.
        If key is "*" return all <key, value> pairs stored locally (or in the entire DHT).
        In a full implementation, you would gather results from all nodes.
        """
        if key == "*":
            # TODO: Propagate query throughout the DHT to gather all data.
            return {
                "status": "query_all",
                "data": self.data
            }
        else:
            hashed_key = hashlib.sha1(key.encode()).hexdigest()
            value = self.data.get(hashed_key)
            return {
                "status": "query",
                "key": key,
                "value": value
            }

    def delete(self, key):
        """
        Delete a <key, value> pair.
        The key is hashed and the corresponding data is removed.
        """
        hashed_key = hashlib.sha1(key.encode()).hexdigest()
        if hashed_key in self.data:
            del self.data[hashed_key]
            # TODO: Handle replication cleanup if replication is used.
            return {
                "status": "deleted",
                "key": key
            }
        else:
            return {
                "status": "not_found",
                "key": key
            }

    def join(self, ip, port):
        """
        Handle a new node joining the DHT.
        In a full implementation, this would involve updating pointers (predecessor and successor)
        and transferring keys to the new node as necessary.
        """
        new_node_id = hashlib.sha1(f"{ip}:{port}".encode()).hexdigest()
        # TODO: Update the ring pointers and transfer data.
        return {
            "status": "join",
            "new_node_id": new_node_id
        }

    def depart(self, ip, port):
        """
        Handle graceful departure of a node.
        Update pointers and reassign keys to ensure the DHT remains consistent.
        """
        departing_node_id = hashlib.sha1(f"{ip}:{port}".encode()).hexdigest()
        # TODO: Update pointers and transfer the departing node's keys.
        return {
            "status": "depart",
            "departed_node_id": departing_node_id
        }

    # Additional methods can be added here to:
    # - Implement finger tables (for faster lookups)
    # - Handle socket communication for peer-to-peer messaging
    # - Perform replication management for data consistency
