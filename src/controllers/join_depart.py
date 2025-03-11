import requests
import helper_functions as hf

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