import requests
import threading
import helper_functions as hf


def insert(self, key, value, client_ip, client_port):
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
        client_url = f"http://{client_ip}:{client_port}/reception"
        
        # Check if the node is primary for the key.
        key_hash = hf.hash_function(key)
        if not (self.node_id == self.predecessor["node_id"] or hf.in_interval(key_hash, self.predecessor["node_id"],
                                                                              self.node_id)):
            next_node = self.find_successor(key_hash)   
            url = f"http://{next_node['ip']}:{next_node['port']}/insert"
            response = requests.post(url, json={"key": key, "value": value, "client_ip": client_ip, "client_port": client_port})
            print(f"[WRITE] Forwarded insert request for key '{key}' to node {next_node['node_id']}")
            return response.json()

        replication_count = self.k_factor
        if self.consistency == "eventual":
            # Write locally into the primary data store.
            self.data_store[key] = self.data_store.get(key, "") + value
            print(f"[WRITE-EC] Node {self.node_id} stored key '{key}' with value '{self.data_store[key]}'")
            # Asynchronously propagate the update if needed.
            # Call the successor to insert replicas.
            t = threading.Thread(target=self.forward_replicate,
                                 args=(key, value, replication_count, False, self.node_id, client_ip, client_port), daemon=True,
                                 name="forward_replicate")
            t.start()  # Start the thread
            ## TODO return from first (primary) node, check
            client_message = {"status": "success", "message": f"Eventually inserted at node {self.ip}:{self.port}", "key": key, "value": value}
            requests.post(client_url, json=client_message)
            return client_message
        else:
            assert self.consistency == "linearizability", "Chain replication is only supported with linearizable consistency"
            # If primary, apply the write locally.
            self.data_store[key] = self.data_store.get(key, "") + value # Concatenate the value if the key already exists
            print(f"[WRITE] Node {self.node_id} stored key '{key}' with value '{self.data_store[key]}'")
            # Call the successor to insert replicas.
            self.forward_replicate(key, value, replication_count, False, self.node_id, client_ip, client_port)
            return {"status": "success", "message": f"Inserted at node {self.ip}:{self.port}", "key": key, "value": value} # Return success message

def insertReplicas(self, key, value, replication_count, join=False, starting_node=None, client_ip=None, client_port=None):
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
        self.forward_replicate(key, value, replication_count, join, starting_node, client_ip, client_port)
        
def forward_replicate(self, key, value, replication_count, join, starting_node, client_ip=None, client_port=None):
        """
        Asynchronously propagate the write for eventual consistency.
        Decrement replication_count before sending to ensure exactly kfactor copies.
        """
        if client_ip:
            client_url = f"http://{client_ip}:{client_port}/reception"
        
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
                        "starting_node": starting_node,
                        "client_ip": client_ip,
                        "client_port": client_port
                    })
                except Exception as e:
                    print(f"[ERROR] Forward replication failed at node {self.node_id}: {e}")
            else:
                #return from last node of the chain, only from linearizability, check
                if(self.consistency == "linearizability" and client_ip):
                    client_message = {"status": "success", "message": f"Inserted at tail node {self.ip}:{self.port}", "key": key, "value": value}
                    requests.post(client_url, json=client_message)
                print(f"Circular replication completed for key '{key}'")
        else:
            #return from last node of the chain, check
            if(self.consistency == "linearizability" and client_ip):
                client_message = {"status": "success", "message": f"Inserted at tail node {self.ip}:{self.port}", "key": key, "value": value}
                requests.post(client_url, json=client_message)
            print(f"Circular replication completed for key '{key}'")

   