import requests
import threading
import helper_functions as hf

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
            if self.consistency == "eventual":
                if self.k_factor > 1:
                    # Open a thread to forward delete replicas and return immediately
                    t = threading.Thread(target=self.forward_delete_replicas,
                                        args=(key, self.k_factor - 1, self.node_id), daemon=True,
                                        name="forward_delete_replicas")
                    t.start()  # Start the thread
                return {"status": "success", "message": f"Deleted '{key}' from node {self.node_id} (eventual consistency)"}
            else:
                assert self.consistency == "linearizability"
                if self.k_factor > 1:
                    # I need to delete the replicas of this key on the other nodes
                    self.forward_delete_replicas(key, self.k_factor - 1, self.node_id)
                return {"status": "success", "message": f"Deleted '{key}' from node {self.node_id} (linearizability)"}
        else:
            next_node = self.find_successor(key_hash)
            url = f"http://{next_node['ip']}:{next_node['port']}/delete"
            response = requests.post(url, json={"key": key})
            return response.json()

def deleteReplicas(self, key, replication_count):
        """
        Delete a key from the replicas.
        """
        result = self.replicas.pop(key, None)
        if result is None:
            return {"status": "success", "message": f"Replica '{key}' not found at node {self.node_id}, stopping propagation"}
        if replication_count > 1:
            self.forward_delete_replicas(key, replication_count - 1, self.node_id)
        return {"status": "success", "message": f"Deleted replicas of '{key}' from node {self.node_id}"}

def forward_delete_replicas(self, key, replication_count, starting_node):
        """
        Propagate the delete for replicas.
        Decrement replication_count before sending to ensure exactly kfactor copies are deleted.
        """
        if self.successor["node_id"] != starting_node:
            try:
                url = f"http://{self.successor['ip']}:{self.successor['port']}/deleteReplicas"
                response = requests.post(url, json={
                    "key": key,
                    "replication_count": replication_count
                })
                if response.status_code != 200:
                    print(f"[ERROR] Forward delete replication failed at node {self.node_id}: {response.text}")
            except Exception as e:
                print(f"[ERROR] Forward delete replication failed at node {self.node_id}: {e}")
        else:
            print(f"Circular delete replication completed for key '{key}'")