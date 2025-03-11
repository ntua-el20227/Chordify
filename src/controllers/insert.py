import requests
import threading
import helper_functions as hf
  
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