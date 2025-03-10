import helper_functions as hf
from controllers.insert import insert, insertReplicas, forward_replicate
from controllers.query import query, query_all_nodes, forward_query_eventual, query_chain
from controllers.delete import delete, deleteReplicas, forward_delete_replicas
from controllers.transfer_keys import transfer_keys, transfer_replicas, generate_replicas, remove_transferred_replicas, shift_replicas, updateReplicas
from controllers.join_depart import join, depart, update_successor, update_predecessor, overlay, get_node_info

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
    
    insert = insert
    insertReplicas = insertReplicas
    forward_replicate = forward_replicate
    query = query
    query_all_nodes = query_all_nodes
    forward_query_eventual = forward_query_eventual
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
    

    
   
    









    

    

    




