import requests
import hashlib


class Node:
    def __init__(self, ip, port, bootstrap_ip, bootstrap_port, consistency="linearizable", k_factor=1):
        self.ip = ip
        self.port = port
        self.node_id = self.hash_function(f"{ip}:{port}")
        self.data_store = {}  # Local key-value store

        # Set consistency mode and replication factor (k-factor)
        self.consistency = consistency
        self.k_factor = k_factor

        # Initially, the node is alone in the ring so its successor and predecessor are itself.
        self.successor = {"ip": self.ip, "port": self.port, "node_id": self.node_id}
        self.predecessor = {"ip": self.ip, "port": self.port, "node_id": self.node_id}

        print(f"[START] Node {self.node_id} at {self.ip}:{self.port}")
        print(f"[CONFIG] Consistency: {self.consistency}, Replication Factor: {self.k_factor}")

        # Always join via the provided bootstrap node.
        if bootstrap_ip and bootstrap_port:
            self.join_bootstrap(bootstrap_ip, bootstrap_port)

    @staticmethod
    def hash_function(key):
        """Compute SHA-1 hash of a key mod 2^16."""
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 16)

    @staticmethod
    def in_interval(x, start, end):
        """
        Check if x is in the circular interval (start, end] modulo 2^16.
        Assumes the values are already reduced modulo 2^16.
        """
        if start < end:
            return start < x <= end
        return x > start or x <= end

    def insert(self, key, value):
        """
        Insert or update a <key, value> pair.
        In this basic implementation, the node checks if it is responsible (i.e. the key's hash falls
        between its predecessor and itself). If so, it performs the update locally (concatenating the value).
        Otherwise, the insert request is forwarded to the successor.
        (Chain replication logic can be integrated here if needed.)
        """
        key_hash = self.hash_function(key)
        if self.node_id == self.predecessor["node_id"] or self.in_interval(key_hash, self.predecessor["node_id"],
                                                                           self.node_id):
            self.data_store[key] = self.data_store.get(key, "") + value
            return {
                "status": "success",
                "message": f"Inserted '{key}' at node {self.node_id}"
            }
        else:
            url = f"http://{self.successor['ip']}:{self.successor['port']}/insert"
            response = requests.post(url, json={"key": key, "value": value})
            return response.json()

    def query(self, key, visited=None):
        """
        Query a key from the DHT.
        For a wildcard query (key == "*"), the query is forwarded around the ring until all data is collected.
        For a specific key, if this node is responsible, it returns the local value;
        otherwise, the request is forwarded to the successor.
        """
        if visited is None:
            visited = []
        if key == "*":
            if self.node_id in visited:
                return {"status": "success", "data": {}}
            visited.append(self.node_id)
            result = self.data_store.copy()
            url = f"http://{self.successor['ip']}:{self.successor['port']}/query"
            response = requests.post(url, json={"key": "*", "visited": visited})
            if response.json().get("status") == "success":
                result.update(response.json().get("data", {}))
            return {"status": "success", "data": result}
        else:
            key_hash = self.hash_function(key)
            if self.node_id == self.predecessor["node_id"] or self.in_interval(key_hash, self.predecessor["node_id"],
                                                                               self.node_id):
                return {"status": "success", "value": self.data_store.get(key, "Key not found")}
            else:
                url = f"http://{self.successor['ip']}:{self.successor['port']}/query"
                response = requests.post(url, json={"key": key})
                return response.json()

    def delete(self, key):
        """
        Delete a key from the DHT.
        If this node is responsible, delete locally; otherwise, forward the request to the successor.
        """
        key_hash = self.hash_function(key)
        if self.node_id == self.predecessor["node_id"] or self.in_interval(key_hash, self.predecessor["node_id"],
                                                                           self.node_id):
            self.data_store.pop(key, None)
            return {"status": "success", "message": f"Deleted '{key}' from node {self.node_id}"}
        else:
            url = f"http://{self.successor['ip']}:{self.successor['port']}/delete"
            response = requests.post(url, json={"key": key})
            return response.json()

    def join(self, new_ip, new_port):
        """
        Handle a join request from a new node.
        Depending on the position of the new node (based on its hash), this node may transfer keys and update its pointers.
        """
        new_node_id = self.hash_function(f"{new_ip}:{new_port}")

        # Case 1: Only one node in the network.
        if self.node_id == self.predecessor["node_id"] and self.node_id == self.successor["node_id"]:
            self.predecessor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}
            self.successor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}
            return {
                "status": "success",
                "new_successor": {"ip": self.ip, "port": self.port, "node_id": self.node_id},
                "new_predecessor": {"ip": self.ip, "port": self.port, "node_id": self.node_id},
                "transferred_keys": {},
                "consistency": self.consistency,
                "k_factor": self.k_factor
            }

        # Case 2: New node belongs between the current node's predecessor and this node.
        if self.in_interval(new_node_id, self.predecessor["node_id"], self.node_id):
            old_predecessor = self.predecessor.copy()
            self.predecessor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}

            # Transfer keys that now belong to the new node.
            keys_to_transfer = {
                k: v for k, v in self.data_store.items()
                if self.in_interval(self.hash_function(k), old_predecessor["node_id"], new_node_id)
            }
            for k in keys_to_transfer:
                del self.data_store[k]

            # Inform the old predecessor to update its successor.
            url = f"http://{old_predecessor['ip']}:{old_predecessor['port']}/update_successor"
            requests.post(url, json={"new_successor": {"ip": new_ip, "port": new_port, "node_id": new_node_id}})
            return {
                "status": "success",
                "new_successor": {"ip": self.ip, "port": self.port, "node_id": self.node_id},
                "new_predecessor": old_predecessor,
                "transferred_keys": keys_to_transfer,
                "consistency": self.consistency,
                "k_factor": self.k_factor
            }

        # Case 3: Forward the join request to the successor.
        url = f"http://{self.successor['ip']}:{self.successor['port']}/join"
        response = requests.post(url, json={"ip": new_ip, "port": new_port})
        return response.json()

    def update_successor(self, new_successor):
        """
        Update this node's successor pointer.
        """
        self.successor = new_successor
        print(f"[UPDATE] Node {self.node_id} updated its successor to {new_successor['node_id']}")
        return {"status": "success", "message": "Successor updated"}

    def update_predecessor(self, new_predecessor):
        """
        Update this node's predecessor pointer.
        """
        self.predecessor = new_predecessor
        print(f"[UPDATE] Node {self.node_id} updated its predecessor to {new_predecessor['node_id']}")
        return {"status": "success", "message": "Predecessor updated"}

    def depart(self):
        """
        Handle graceful departure of this node.
        Inform neighbors, transfer keys, and clear local state.
        """
        # Inform predecessor to update its successor.
        url_pred = f"http://{self.predecessor['ip']}:{self.predecessor['port']}/update_successor"
        requests.post(url_pred, json={"new_successor": self.successor})
        # Inform successor to update its predecessor.
        url_succ = f"http://{self.successor['ip']}:{self.successor['port']}/update_predecessor"
        requests.post(url_succ, json={"new_predecessor": self.predecessor})
        # Transfer keys to the successor.
        url_transfer = f"http://{self.successor['ip']}:{self.successor['port']}/transfer_keys"
        requests.post(url_transfer, json={"keys": self.data_store})
        self.data_store.clear()
        self.successor = None
        self.predecessor = None
        print(f"[DEPART] Node {self.node_id} departed gracefully.")
        return {"status": "success", "message": f"Node {self.node_id} departed gracefully"}

    def overlay(self, visited=None):
        """
        Retrieve the overlay (list of nodes) starting from this node.
        The 'visited' list prevents infinite loops.
        """
        if visited is None:
            visited = []
        if self.node_id in visited:
            return {"status": "success", "overlay": []}
        visited.append(self.node_id)
        overlay_list = [{"node_id": self.node_id, "ip": self.ip, "port": self.port}]
        url = f"http://{self.successor['ip']}:{self.successor['port']}/overlay"
        response = requests.get(url, params={"visited_ids": visited})
        if response.json().get("status") == "success":
            overlay_list.extend(response.json().get("overlay", []))
        return {"status": "success", "overlay": overlay_list}

    def join_bootstrap(self, bootstrap_ip, bootstrap_port):
        """
        Join an existing Chord ring using a bootstrap node.
        Update this node's successor, predecessor, and local key store based on the response.
        """
        url = f"http://{bootstrap_ip}:{bootstrap_port}/join"
        response = requests.post(url, json={"ip": self.ip, "port": self.port})
        resp_json = response.json()
        if resp_json.get("status") == "success":
            self.successor = resp_json["new_successor"]
            self.predecessor = resp_json["new_predecessor"]
            self.data_store.update(resp_json.get("transferred_keys", {}))
            print(f"[JOINED] Successor: {self.successor['node_id']}, Predecessor: {self.predecessor['node_id']}")
        else:
            print("[JOIN FAILED]", resp_json)
