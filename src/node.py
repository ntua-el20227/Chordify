import requests
import hashlib
import threading
import helper_functions as hf


class Node:

    def __init__(self, ip, port, consistency="linearizability", k_factor=1, successor=None, predecessor=None,
                 data_store={}, replicas={}):

        self.ip = ip
        self.port = port
        self.node_id = hf.hash_function(f"{ip}:{port}")
        self.data_store = data_store

        # Set consistency mode and replication factor (k-factor)
        self.consistency = consistency
        self.k_factor = int(k_factor)
        self.replicas = replicas  # Local replica store
        if successor and predecessor:
            self.successor = successor
            self.predecessor = predecessor
        else:
            # Initially, the node is alone in the ring so its successor and predecessor are itself.
            self.successor = {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            self.predecessor = {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            # self.predecessor = Node(self.predecessor["ip"], self.predecessor["port"])
            # self.successor = Node(self.successor["ip"], self.successor["port"])

        print(f"[START] Node {self.node_id} at {self.ip}:{self.port}")
        print(f"[CONFIG] Consistency: {self.consistency}, Replication Factor: {self.k_factor}")

    def insert(self, key, value):
        """
        Primary insertion method.

        For eventual consistency:
          - Write locally and return immediately.
          - Propagate the update asynchronously (decrementing replication_count so that
            exactly kfactor replicas are updated).

        For chain replication (linearizable consistency):
          - Check if this node is primary (responsible) for the key.
            If not, forward the request.
          - If primary, write locally and then call insertReplicas if replication_count > 1.
        """
        # Check if the node is primary for the key.
        key_hash = hf.hash_function(key)
        if not (self.node_id == self.predecessor["node_id"] or hf.in_interval(key_hash, self.predecessor["node_id"],
                                                                              self.node_id)):
            url = f"http://{self.successor['ip']}:{self.successor['port']}/insert"
            response = requests.post(url, json={"key": key, "value": value})
            print(f"[WRITE] Forwarded insert request for key '{key}' to node {self.successor['node_id']}")
            return response.json()

        replication_count = self.k_factor
        if self.consistency == "eventual":
            # Write locally into the primary data store.
            self.data_store[key] = self.data_store.get(key, "") + value
            print(f"[WRITE-EC] Node {self.node_id} stored key '{key}' with value '{self.data_store[key]}'")
            # Asynchronously propagate the update if needed.
            # Call the successor to insert replicas.
            t = threading.Thread(target=self.forward_replicate,
                                 args=(key, value, replication_count, False, self.node_id), daemon=True,
                                 name="forward_replicate")
            t.start()  # Start the thread
            return {"status": "success", "message": f"Eventually inserted '{key}' at node {self.node_id}"}
        else:
            assert self.consistency == "linearizability", "Chain replication is only supported with linearizable consistency"
            # If primary, apply the write locally.
            self.data_store[key] = self.data_store.get(key, "") + value
            print(f"[WRITE] Node {self.node_id} stored key '{key}' with value '{self.data_store[key]}'")
            # Call the successor to insert replicas.
            self.forward_replicate(key, value, replication_count, False, self.node_id)
            return {"status": "success",
                    "message": f"Inserted '{key}' at node {self.node_id}"}  # Return success message

    def insertReplicas(self, key, value, replication_count, join=False, starting_node=None):
        """
        Replica insertion method for chain replication.

        This method stores the key into the replicas dictionary on the current node,
        then (if needed) forwards the request to the successor with a decremented replication_count.
        """
        # Check if the node is the starting point after completing a circle
        if key in self.data_store:
            return {"status": "success",
                    "message": f"Kfactor was bigger than the number of nodes in the ring, key '{key}' was already inserted"}

        # Write the replica locally saving its value and its replica count.
        if not join:
            # If the key already exists, append the new value to the existing one when we have insertion replicas
            self.replicas[key] = self.replicas.get(key, ("", 0))[0] + value, int(replication_count)
            print(
                f"[WRITE_INSERT] Node {self.node_id} stored replica key '{key}' with value '{self.replicas[key]}' and replica_count:{replication_count}")
        else:
            # If the key already exists, append the new value to the existing one when we have join replicas
            self.replicas[key] = (value, int(replication_count))
            print(
                f"[WRITE_JOIN/DEPART] Node {self.node_id} stored replica key '{key}' with value '{self.replicas[key]}' and replica_count:{replication_count}")
        # If more replicas are needed, forward the request.
        self.forward_replicate(key, value, replication_count, join, starting_node)

    def forward_replicate(self, key, value, replication_count, join, starting_node):
        """
        Asynchronously propagate the write for eventual consistency.
        Decrement replication_count before sending to ensure exactly kfactor copies.
        """
        if self.successor["node_id"] != starting_node:
            print(key)
            print(value)
            if int(replication_count) > 1:
                try:
                    url = f"http://{self.successor['ip']}:{self.successor['port']}/insertReplicas"
                    requests.post(url, json={
                        "key": key,
                        "value": value,
                        "replication_count": int(replication_count) - 1,
                        "join": join,
                        "starting_node": starting_node
                    })
                except Exception as e:
                    print(f"[ERROR] Forward replication failed at node {self.node_id}: {e}")
        else:
            print(f"Circular replication completed for key '{key}'")

    def query(self, key):
        """
        Query operation supporting both eventual and linearizable consistency.

        For eventual consistency:
          - Look up the key in the local primary store, and if not found,
            in the replica store. If still not found, forward the query.

        For linearizable consistency (chain replication):
          - If no read_count is provided, this is the initial query.
            Check if this node is responsible for the key. If not, forward
            the query to the responsible node. If responsible, start the chain
            query with a replication count of self.k_factor.
          - If read_count is provided, we are already in the chain query.
        """
        key_hash = hf.hash_function(key)
        should_be_here = self.node_id == self.predecessor["node_id"] or hf.in_interval(key_hash,
                                                                                       self.predecessor["node_id"],
                                                                                       self.node_id)

        if self.consistency == "eventual":
            #Handle eventual consistency query by checking local primary and replica stores.

            # Check primary data store if the key is in the interval.
            if should_be_here:
                primary_value = self.data_store.get(key, "Key not found")
                if primary_value != "Key not found":
                    print(f"[READ-EC] Node {self.node_id} found primary for '{key}' with value '{primary_value}'")
                    return {"status": "success", "value": primary_value}
            # Check replica store (stale values are acceptable).
            replica_value, _ = self.replicas.get(key, ("Key not found", 0))
            if replica_value != "Key not found":
                print(f"[READ-EC] Node {self.node_id} found replica for '{key}' with value '{replica_value}'")
                return {"status": "success", "value": replica_value}

            # Not found locally; forward the query.
            return self.forward_query_eventual(key)
        else:
            assert self.consistency == "linearizability", "Wrong consistency mode"
            # Linearizable consistency

            # Corner case: if the node is the only one in the ring
            if self.node_id == self.predecessor["node_id"]:
                if key in self.data_store:
                    return {"status": "success", "value": self.data_store[key]}
                else:
                    return {"status": "error", "message": f"Key '{key}' not found in the only node in the ring"}

            # Initial query: ensure the query starts at the node responsible for the key.
            if not hf.in_interval(key_hash, self.predecessor["node_id"], self.node_id):
                # Forward the query to the responsible node.
                url = f"http://{self.successor['ip']}:{self.successor['port']}/query"
                response = requests.post(url, json={"key": key})
                return response.json()
            else:
                return self.query_chain(self.successor['ip'], self.successor['port'], key, self.k_factor - 1,
                                        self.node_id)

    def forward_query_eventual(self, key):
        """Forward an eventual consistency query to the successor."""
        try:
            url = f"http://{self.successor['ip']}:{self.successor['port']}/query"
            response = requests.post(url, json={"key": key})
            return response.json()
        except Exception as e:
            return {"status": "error", "message": f"Eventual query forwarding failed: {e}"}

    def query_chain(self, ip, port, key, replication_count, starting_id):
        """
        Handle a linearizable consistency query as part of a chain replication.

        Check the local replica store. If the replica's replication counter is 1,
        then this node is the tail and returns the final value. Otherwise, forward
        the query to the successor with a decremented replication count.
        """
        # Get the info of current node
        url = f"http://{ip}:{port}/node_info"
        response = requests.get(url)
        if response.status_code != 200:
            return {"status": "error", "message": f"Failed to get node info from successor: {response.text}"}
        replicas = response.json().get("replicas")  # Get the replicas of the current node
        successor = response.json().get("successor")  # Get the successor of the current node in case of mismatch
        if key in replicas:
            replica_value, rep_count = replicas[key]
        else:
            replica_value = "Key not found"
            rep_count = 0
        if replica_value != "Key not found" and (
                rep_count == 1 or successor['node_id'] == starting_id):  # Only the tail node returns the final value.
            print(f"[READ-LIN] Tail node {port} returning final value '{replica_value}' for key '{key}'")
            return {"status": "success", "value": replica_value}
        else:
            if replication_count > 1:
                print(f"[READ-LIN] Node {port} forwarding query for key '{key}' to node {successor['port']}")
                return self.query_chain(successor['ip'], successor['port'], key, replication_count - 1, starting_id)
            else:
                return {"status": "error", "message": f"Key '{key}' not found in linearizable chain"}

    def delete(self, key):
        """
        Delete a key from the DHT.
        If this node is responsible, delete locally; otherwise, forward the request to the successor.
        """
        key_hash = hf.hash_function(key)
        should_be_here = self.node_id == self.predecessor["node_id"] or hf.in_interval(key_hash,
                                                                                       self.predecessor["node_id"],
                                                                                       self.node_id)
        if should_be_here:
            self.data_store.pop(key, "Key not inserted")
            if self.k_factor > 1:
                # I need to delete the replicas of this key on the other nodes
                replica_count = self.k_factor
                url = f"http://{self.successor['ip']}:{self.successor['port']}/deleteReplicas"
                requests.post(url, json={"key": key, "replica_count": replica_count})

            return {"status": "success", "message": f"Deleted '{key}' from node {self.node_id}"}
        else:
            url = f"http://{self.successor['ip']}:{self.successor['port']}/delete"
            response = requests.post(url, json={"key": key})
            return response.json()

    def deleteReplicas(self, key, replication_count):
        """
        Delete a key from the replicas.
        """
        self.replicas.pop(key, None)
        if replication_count > 1:
            url = f"http://{self.successor['ip']}:{self.successor['port']}/deleteReplicas"
            requests.post(url, json={"key": key, "replica_count": replication_count - 1})
        return {"status": "success", "message": f"Deleted replicas of '{key}' from node {self.node_id}"}

    # JOIN RELATED METHODS
    def join(self, new_ip, new_port):
        """
        Handle a join request from a new node.

        If the new node's ID is between this node's predecessor and this node,
        then:
          - Update the predecessor pointer.
          - Transfer keys (and their replicas) that now belong to the new node.
          - Shift the replica chain so that the (k_factor - 1) nodes after the new node
            hold replicas for the transferred keys.
          - Inform the old predecessor to update its successor pointer.

        Otherwise, forward the join request to the successor.
        """
        new_node_id = hf.hash_function(f"{new_ip}:{new_port}")
        # Case 1: New node is between this node and its predecessor.
        if hf.in_interval(new_node_id, self.predecessor["node_id"], self.node_id):
            # Save old predecessor for later use.
            old_predecessor = self.predecessor.copy()
            replicas_to_transfer = {}
            # Transfer keys that now belong to the new node.
            # Keys with hash in (old_predecessor, new_node_id] should be transferred.
            keys_to_transfer = {
                k: v for k, v in self.data_store.items()
                if hf.in_interval(hf.hash_function(k), old_predecessor["node_id"], new_node_id)
            }
            for k in keys_to_transfer:
                del self.data_store[k]
            if self.k_factor > 1:
                # Also transfer any replicas for these keys.
                replicas_to_transfer = {
                    k: v for k, v in self.replicas.items()
                }

                self.shift_replicas(keys_to_transfer, replicas_to_transfer, self.node_id)

            # for key in data_store_copy.keys():
            #     url_node_info = f"http://{old_predecessor['ip']}:{old_predecessor['port']}/node_info"
            #     node_info_resp = requests.get(url_node_info).json()
            #     pred_replicas = node_info_resp.get("replicas")
            #     if key in pred_replicas:
            #         a = pred_replicas[key][1]
            #
            #         if  a > 1:
            #             print(f"key {key} has {a} replicas")
            #             replicas_to_transfer[key] = (self.data_store[key], a-1)

            # Update this node's predecessor pointer.
            self.predecessor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}
            # Inform the old predecessor to update its successor pointer
            try:
                url = f"http://{old_predecessor['ip']}:{old_predecessor['port']}/update_successor"
                requests.post(url, json={"new_successor": {"ip": new_ip, "port": new_port, "node_id": new_node_id}})
            except Exception as e:
                print(f"[ERROR] Failed to update old predecessor's successor: {e}")

            # for k in keys_to_transfer:
            #     self.replicas[k] = (keys_to_transfer[k], self.k_factor - 1)

            return {
                "status": "success",
                "new_successor": {"ip": self.ip, "port": self.port, "node_id": self.node_id},
                "new_predecessor": old_predecessor,
                "transferred_keys": keys_to_transfer,
                "transferred_replicas": replicas_to_transfer,
                "consistency": self.consistency,
                "k_factor": self.k_factor
            }
        else:
            # Case 2 : Forward the join request to the successor.
            url = f"http://{self.successor['ip']}:{self.successor['port']}/join"
            response = requests.post(url, json={"ip": new_ip, "port": new_port})
            return response.json()

    def transfer_replicas(self, replicas):
        """
        Transfer replicas to this node.
        Update the local replica store with the provided replicas.
        """
        for key, (value, rep_count) in replicas.items():
            if key not in self.data_store:
                self.replicas[key] = (value, rep_count)
                print(
                    f"[TRANSFER REPLICAS] Node {self.node_id} received replica for key '{key}' with count {rep_count}")
                # Propagate only if there is more than one node in the ring.
                self.forward_replicate(key, value, rep_count, True, self.node_id)

    def updateReplicas(self, replicas, new_node_id):
        """
        Update the local replica store with the provided replicas.
        Then propagate the update to the successor, unless this is the only node.
        """
        for key, (value, rep_count) in replicas.items():
            self.replicas[key] = (value, rep_count)
            print(f"[UPDATE REPLICA] Node {self.node_id} updated replica for key '{key}' with count {rep_count}")
        # Propagate only if there is more than one node in the ring.
        if self.successor["node_id"] != self.node_id:
            try:
                url = f"http://{self.successor['ip']}:{self.successor['port']}/updateReplicas"
                requests.post(url, json={"replicas": replicas, "new_node_id": new_node_id})
            except Exception as e:
                print(f"[ERROR] Failed to propagate updateReplicas: {e}")

        return {"status": "success", "message": "Replicas updated", "node_id": self.node_id}

    def shift_replicas(self, data, replicas, starting_node):
        """
        For every key in 'keys' that exists in the local replica store:
          - Decrement the replication count.
          - If the count reaches 0, remove the replica.
        Then propagate the shift to the successor (unless this is the only node).
        """
        for key in data:
            if key in self.replicas.keys():
                value, rep_count = self.replicas[key]
                new_count = rep_count - 1
                if new_count == 0:
                    assert new_count >= 0, "Replication count cannot be negative"
                    del self.replicas[key]
                    print(f"[SHIFT REPLICA] Node {self.node_id} removed replica for key '{key}'")
                else:
                    self.replicas[key] = (value, new_count)
                    print(
                        f"[SHIFT REPLICA] Node {self.node_id} decremented replica count for key '{key}' to {new_count}")
        for key in replicas:
            if key in self.replicas.keys() and replicas[key][1] >= self.replicas[key][1]:
                value, rep_count = self.replicas[key]
                new_count = rep_count - 1
                if new_count == 0:
                    assert new_count >= 0, "Replication count cannot be negative"
                    del self.replicas[key]
                    print(f"[SHIFT REPLICA] Node {self.node_id} removed replica for key '{key}'")
                else:
                    self.replicas[key] = (value, new_count)
                    print(
                        f"[SHIFT REPLICA] Node {self.node_id} decremented replica count for key '{key}' to {new_count}")
        # Propagate only if there is more than one node.
        if self.successor["node_id"] != starting_node:
            try:
                url = f"http://{self.successor['ip']}:{self.successor['port']}/shift_replicas"
                requests.post(url, json={"keys": data, "replicas": replicas, "starting_node": starting_node})
            except Exception as e:
                print(f"[ERROR] Failed to propagate shiftReplicas: {e}")
        return {"status": "success", "message": "Replicas shifted", "node_id": self.node_id}

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

    def transfer_keys(self, keys):
        """
        Transfer keys to this node.
        """
        self.data_store.update(keys)
        return {"status": "success", "message": "Keys transferred successfully"}

    def generate_replicas(self, keys):
        """
        Generate replicas for a key-value pair.
        """
        # Replicate the transferred keys to the k_factor - 1 nodes after the new node.
        for key, value in keys.items():
            self.forward_replicate(key, value, self.k_factor, True, self.node_id)

    # TODO: Implement the depart method for replication
    def depart(self):
        """
        Handle graceful departure of this node.
        Inform neighbors, transfer keys, and clear local state.
        """
        # Inform predecessor to update its successor.
        url_pred = f"http://{self.predecessor['ip']}:{self.predecessor['port']}/update_successor"
        requests.post(url_pred, json={"new_successor": self.successor})
        # TODO : 1 node left when departing
        # Inform successor to update its predecessor.
        url_succ = f"http://{self.successor['ip']}:{self.successor['port']}/update_predecessor"
        requests.post(url_succ, json={"new_predecessor": self.predecessor})
        # Transfer keys to the successor.
        url_transfer = f"http://{self.successor['ip']}:{self.successor['port']}/transfer_keys"
        requests.post(url_transfer, json={"keys": self.data_store})
        # Transfer replicas to the successor.
        url_transfer_replicas = f"http://{self.successor['ip']}:{self.successor['port']}/transfer_replicas"
        requests.post(url_transfer_replicas, json={"replicas": self.replicas})
        # Make the successor generate replicas for the transferred keys.
        url_replicas = f"http://{self.successor['ip']}:{self.successor['port']}/generate_replicas"
        requests.post(url_replicas, json={"keys": self.data_store})

        # Make the successor update its replicas by removing the keys that were transferred.
        url_update_replicas = f"http://{self.successor['ip']}:{self.successor['port']}/remove_transferred_replicas"
        requests.post(url_update_replicas, json={"keys": self.data_store})

        # Clear local state
        self.data_store.clear()
        self.replicas.clear()
        self.successor = None
        self.predecessor = None
        # suicide
        print(f"[DEPART] Node {self.node_id} departed gracefully.")
        return {"status": "success", "message": f"Node {self.node_id} departed gracefully"}

    def remove_transferred_replicas(self, data):
        """
        Remove the replicas of the transferred keys.
        """
        for key in data.keys():
            self.replicas.pop(key, None)
        return {"status": "success", "message": "Transferred replicas removed"}

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

    def get_node_info(self):
        """
        Return the node's information (ID, IP, port, successor, predecessor).
        """
        return {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "successor": self.successor,
            "predecessor": self.predecessor,
            "data_store": self.data_store,
            "replicas": self.replicas
        }
