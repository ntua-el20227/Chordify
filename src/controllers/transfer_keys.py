import requests

def transfer_keys(self, keys):
        """
        Transfer keys to this node.
        """
        self.data_store.update(keys)
        return {"status": "success", "message": "Keys transferred successfully"}
    
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
                
def generate_replicas(self, keys):
        """
        Generate replicas for a key-value pair.
        """
        # Replicate the transferred keys to the k_factor - 1 nodes after the new node.
        for key, value in keys.items():
            self.forward_replicate(key, value, self.k_factor, True, self.node_id)
            
def remove_transferred_replicas(self, data):
        """
        Remove the replicas of the transferred keys.
        """
        for key in data.keys():
            self.replicas.pop(key, None)
        return {"status": "success", "message": "Transferred replicas removed"}
    
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