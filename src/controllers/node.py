import requests
import helper_functions as hf
from controllers.insert import insert, insertReplicas, forward_replicate
from controllers.query import query, query_all_nodes, query_chain
from controllers.delete import delete, deleteReplicas, forward_delete_replicas
from controllers.transfer_keys import transfer_keys, transfer_replicas, generate_replicas, remove_transferred_replicas, shift_replicas, updateReplicas
from controllers.join_depart import join, depart, update_successor, update_predecessor, overlay, get_node_info

class Node:

    def __init__(self, ip, port, consistency="linearizability", k_factor=1, successor=None, predecessor=None,
                 data_store={}, replicas={}, m=16):

        self.ip = ip
        self.port = port
        self.node_id = hf.hash_function(f"{ip}:{port}")
        self.data_store = data_store

        # Set consistency mode and replication factor (k-factor)
        self.consistency = consistency
        self.k_factor = int(k_factor)
        self.replicas = replicas  # Local replica store
        self.m = m  # Number of bits in the hash space

        if successor and predecessor:
            self.successor = successor
            self.predecessor = predecessor
        else:
            # Initially, the node is alone in the ring so its successor and predecessor are itself.
            self.successor = {"ip": self.ip, "port": self.port, "node_id": self.node_id}
            self.predecessor = {"ip": self.ip, "port": self.port, "node_id": self.node_id}

        # Initialize the finger table
        self.finger_table = [self.successor for _ in range(16)]

        print(f"[START] Node {self.node_id} at {self.ip}:{self.port}")
        print(f"[CONFIG] Consistency: {self.consistency}, Replication Factor: {self.k_factor}")

    
    def find_successor(self, id):
        """
        Find the successor of the given id using the finger table.
        """
        if hf.in_interval(id, self.node_id, self.successor["node_id"]):
            return self.successor
        else:
            closest_preceding_node = self.closest_preceding_node(id)
            url = f"http://{closest_preceding_node['ip']}:{closest_preceding_node['port']}/find_successor"
            response = requests.post(url, json={"id": id})
            return response.json()

    def closest_preceding_node(self, id):
        """
        Find the closest preceding node for the given id using the finger table.
        """
        for i in range(15, -1, -1):
            if hf.in_interval(self.finger_table[i]["node_id"], self.node_id, id):
                return self.finger_table[i]
        return self.successor
    
    def initialize_finger_table(self):
        """
        Initialize the finger table for the node and update the finger tables of other nodes.
        """
        for i in range(16):
            start = (self.node_id + 2**i) % (2**16)
            self.finger_table[i] = self.find_successor(start)
        
        print(f"[FINGER TABLE] Node {self.node_id} initialized finger table.")
        
        # Get the list of all nodes in the overlay
        url = f"http://{self.ip}:{self.port}/overlay"
        response = requests.get(url)
        overlay_response = response.json()
        overlay_list = overlay_response.get('overlay', [])

        # Update the finger tables of other nodes
        for node in overlay_list:
            if node["node_id"] != self.node_id:
                url = f"http://{node['ip']}:{node['port']}/update_finger_table"
                requests.post(url)
    
    def update_finger_table(self):
        """
        Update the finger table of the node.
        """
        for i in range(16):
            start = (self.node_id + 2**i) % (2**16)
            self.finger_table[i] = self.find_successor(start)
        
        print(f"[FINGER TABLE] Node {self.node_id} updated finger table.")
        
        
    insert = insert
    insertReplicas = insertReplicas
    forward_replicate = forward_replicate
    query = query
    query_all_nodes = query_all_nodes
    query_chain = query_chain
    delete = delete
    deleteReplicas = deleteReplicas
    forward_delete_replicas = forward_delete_replicas
    transfer_keys = transfer_keys
    transfer_replicas = transfer_replicas
    generate_replicas = generate_replicas
    remove_transferred_replicas = remove_transferred_replicas
    shift_replicas = shift_replicas
    updateReplicas = updateReplicas
    join = join
    depart = depart
    update_successor = update_successor
    update_predecessor = update_predecessor
    overlay = overlay
    get_node_info = get_node_info