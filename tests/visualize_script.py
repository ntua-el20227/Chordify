import requests
from graphviz import Digraph
import time


def get_node_info(ip, port):
    try:
        response = requests.get(f"http://{ip}:{port}/node_info")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error getting info from {ip}:{port} - {e}")
    return None


def visualize_chord_ring(chord_ring):
    """
    Traverses the chord ring using each node's successor pointer and visualizes the ring.
    The graph will show each node (with its id and ip:port) and an arrow from each node to its successor.
    """
    dot = Digraph(comment='Chord Ring')
    visited = set()

    # Start with the bootstrap node (assumed to be the first in the list)
    start_node = chord_ring["nodes"][0]
    current_ip = start_node["ip"]
    current_port = start_node["port"]
    node_info = get_node_info(current_ip, current_port)

    if node_info is None:
        print("Unable to fetch info from the bootstrap node")
        return dot

    start_id = node_info.get("node_id", f"{current_ip}:{current_port}")
    current_id = start_id
    # Traverse the ring until we reach the starting node again.
    while current_id not in visited:
        visited.add(current_id)
        # Use the current node's data to create a node in the graph.
        label = f"{current_id}\n{current_ip}:{current_port}"
        dot.node(str(current_id), label)

        # Get successor info.
        successor = node_info.get("successor")
        if not successor:
            print(f"No successor found for {current_id}. Aborting traversal.")
            break

        succ_ip = successor.get("ip")
        succ_port = successor.get("port")
        succ_id = successor.get("node_id", f"{succ_ip}:{succ_port}")
        dot.edge(str(current_id), str(succ_id))

        # Prepare to move to the next node.
        current_ip, current_port = succ_ip, succ_port
        node_info = get_node_info(current_ip, current_port)
        if node_info is None:
            print(f"Unable to fetch info from {succ_ip}:{succ_port}")
            break
        current_id = node_info.get("node_id", f"{current_ip}:{current_port}")
        time.sleep(0.5)  # Optional pause for clarity/debugging

    return dot




