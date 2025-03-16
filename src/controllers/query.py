import requests
import helper_functions as hf
from datetime import datetime


def query(self, key, client_ip, client_port):
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
        client_url = f"http://{client_ip}:{client_port}/reception"

        if key == "*":
             return self.query_all_nodes()
            
        key_hash = hf.hash_function(key)
        
        if self.consistency == "eventual":
            
            should_be_here = self.node_id == self.predecessor["node_id"] or hf.in_interval(key_hash,
                                                                                       self.predecessor["node_id"],
                                                                                       self.node_id)
            #Handle eventual consistency query by checking local primary and replica stores.

            # Check primary data store if the key is in the interval.
            if should_be_here:
                primary_value = self.data_store.get(key, "Key not found")
                if primary_value != "Key not found":
                    print(f"[READ-EC] Node {self.node_id} found primary for '{key}' with value '{primary_value}'")
                    ##TODO return original, check             
                    client_message = {"status": f"success from  NODE {self.ip}:{self.port}", "key": key, "value": primary_value, "timestamp": datetime.now().strftime("%H:%M:%S")}

                else:
                    ##TODO not found, check
                    client_message = {"status": "error", "message": f"Key '{key}' not found in node {self.node_id}"}

                requests.post(client_url, json=client_message)
                return client_message    
            # Check replica store (stale values are acceptable).
            replica_value, _ = self.replicas.get(key, ("Key not found", 0))
            if replica_value != "Key not found":
                print(f"[READ-EC] Node {self.node_id} found replica for '{key}' with value '{replica_value}'")
                ## TODO return replica, check               
                client_message = {"status": f"success from  replica NODE {self.ip}:{self.port}", "key": key, "replica value": replica_value, "timestamp": datetime.now().strftime("%H:%M:%S")}
                requests.post(client_url, json=client_message)
                return client_message
            
            # Forward the query to the responsible node.
            next_node = self.find_successor(key_hash)
            url = f"http://{next_node['ip']}:{next_node['port']}/query"
            response = requests.post(url, json={"key": key, "client_ip": client_ip, "client_port": client_port})
            return response.json()
            
        else:
            assert self.consistency == "linearizability"
            # Linearizable consistency

            # Corner case: if the node is the only one in the ring
            if self.node_id == self.predecessor["node_id"]:
                if key in self.data_store:
                    ## TODO return original, one node, check
                    client_message = {"status": f"success from  NODE {self.ip}:{self.port}", "key": key, "value": self.data_store[key], "timestamp": datetime.now().strftime("%H:%M:%S")}
                    requests.post(client_url, json=client_message)
                    return client_message
                
                else:
                    ## TODO not found, check
                    client_message = {"status": "error", "message": f"Key '{key}' not found in the only node in the ring"}
                    requests.post(client_url, json=client_message)
                    return client_message

            # Initial query: ensure the query starts at the node responsible for the key.
            if not hf.in_interval(key_hash, self.predecessor["node_id"], self.node_id):
                # Forward the query to the responsible node.
                next_node = self.find_successor(key_hash)
                url = f"http://{next_node['ip']}:{next_node['port']}/query"
                response = requests.post(url, json={"key": key, "client_ip": client_ip, "client_port": client_port})
                return response.json()
            else:
                # case of kfactor 1.
                if(self.k_factor == 1):
                    if key in self.data_store:
                        client_message = {"status": f"success from  NODE {self.ip}:{self.port}", "key": key, "value": self.data_store[key], "timestamp": datetime.now().strftime("%H:%M:%S")}
                        requests.post(client_url, json=client_message)
                        return client_message
                    else:
                        client_message = {"status": "error", "message": f"Key '{key}' not found in node {self.node_id}"}
                        requests.post(client_url, json=client_message)
                        return client_message
                    
                return self.query_chain(self.successor['ip'], self.successor['port'], key, self.k_factor - 1,
                                        self.node_id, client_ip, client_port)
                
def query_all_nodes(self):
        """
        Retrieve all data and replica values from all nodes in the DHT.
        """
        from controllers.node import Node # Avoid circular import
        
        all_data = []
        current_node = self
        starting_node_id = self.node_id

        while True:
            node_info = {
                "node_id": current_node.node_id,
                "data": current_node.data_store,
                "replica_values": current_node.replicas
            }
            all_data.append(node_info)

            if current_node.successor["node_id"] == starting_node_id:
                break
            url = f"http://{current_node.successor['ip']}:{current_node.successor['port']}/node_info"
            response = requests.get(url)
            if response.status_code != 200:
                return {"status": "error", "message": f"Failed to get node info from successor: {response.text}"}
            node_info = response.json()
            current_node = Node(node_info["ip"], node_info["port"], data_store=node_info["data_store"],
                                replicas=node_info["replicas"], successor=node_info["successor"],
                                predecessor=node_info["predecessor"])
            
            
        return {"status": "success", "all_data": all_data}

def query_chain(self, ip, port, key, replication_count, starting_id, client_ip, client_port):
        """
        Handle a linearizable consistency query as part of a chain replication.

        Check the local replica store. If the replica's replication counter is 1,
        then this node is the tail and returns the final value. Otherwise, forward
        the query to the successor with a decremented replication count.
        """
        client_url = f"http://{client_ip}:{client_port}/reception"
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
            ## TODO return original from tail, check
            client_message = {"status": f"success from TAIL NODE {ip}:{port}", "key": key, "value": replica_value, "timestamp": datetime.now().strftime("%H:%M:%S")}
            requests.post(client_url, json=client_message)
            return client_message
        else:
            if replication_count > 1:
                print(f"[READ-LIN] Node {port} forwarding query for key '{key}' to node {successor['port']}")
                return self.query_chain(successor['ip'], successor['port'], key, replication_count - 1, starting_id, client_ip, client_port)
            else:
                ## TODO not found, check
                client_message = {"status": "error", "message": f"Key '{key}' not found in linearizable chain"}
                requests.post(client_url, json=client_message)
                return client_message

 