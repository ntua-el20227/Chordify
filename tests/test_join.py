import pytest
import time
import requests
import subprocess
import hashlib
from tests.kill_ports import kill_ports

from tests.visualize_script import visualize_chord_ring


def hash_function(key):
    """Compute SHA-1 hash of a key mod 2^16."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 16)


keys = []

# Path to the app file and the python executable
APP_PATH = "../src/app.py"
PYTHON_PATH = "python.exe"

k_factor = 4
consistency = "linearizability"

@pytest.fixture(scope="module")
def chord_ring():
    processes = []
    # Number of nodes and ports to use
    nodes = [
        {"ip": "127.0.0.1", "port": 5000},
        {"ip": "127.0.0.1", "port": 5001},
        {"ip": "127.0.0.1", "port": 5002},
        {"ip": "127.0.0.1", "port": 5003},
        {"ip": "127.0.0.1", "port": 5004},
        {"ip": "127.0.0.1", "port": 5005},
        {"ip": "127.0.0.1", "port": 5006},
    ]
    time.sleep(4)
    # Start bootstrap node
    bootstrap_cmd = f"{PYTHON_PATH} {APP_PATH} {nodes[0]['ip']} {nodes[0]['port']}"
    processes.append(subprocess.Popen(bootstrap_cmd, shell=True))

    time.sleep(3)  # Allow bootstrap to start

    # Start other nodes
    bootstrap_ip, bootstrap_port = nodes[0]['ip'], nodes[0]['port']
    for node in nodes[1:]:
        join_cmd = f"{PYTHON_PATH} {APP_PATH} {node['ip']} {node['port']} {bootstrap_ip} {bootstrap_port}"
        processes.append(subprocess.Popen(join_cmd, shell=True))
        time.sleep(2)  # Allow each node to start

    chord_graph = visualize_chord_ring(nodes)
    # Render and view the graph (this creates a file named chord_ring.gv.pdf and opens it)
    chord_graph.render('chord_ring.gv', view=True)

    yield {"nodes": nodes}

    ports = [node["port"] for node in nodes]
    kill_ports(ports)


def test_insertions(chord_ring, insert_filepath="C:\\Users\\koust\\PycharmProjects\\Chordify\\data\\insert_0.txt"):
    # Read the keys from the file
    global keys
    with open(insert_filepath, "r") as f:
        keys = [line.strip() for line in f if line.strip()]
    # Insert each key into the ring using the bootstrap node
    bootstrap_ip = chord_ring["nodes"][0]['ip']
    bootstrap_port = chord_ring["nodes"][0]['port']
    for key in keys:
        value = f"value_for_{key}"
        response = requests.post(
            f"http://{bootstrap_ip}:{bootstrap_port}/insert",
            json={"key": key, "value": value}
        )
        assert response.status_code == 200
        print(response.json())
        time.sleep(0.5)  # Allow time for the insertion to propagate


def test_replicas_upon_insertion(chord_ring):
    """
    New test:
    - Reads keys from a .txt file (one key per line)
    - Inserts each key into the chord ring
    - Verifies that each key (and its replica) exists in the ring
    - Simulates a node departure and rejoining
    - Verifies again that all keys and replicas are correctly available,
      and that for every key in every node's data_store, the next k_factor-1 nodes
      (traversing via each node's successor) have its replicas with replication_count:
      k_factor-1, k_factor-2, ..., 1.
    """

    # --- Initial verification of keys and replica counts ---
    # For each key, verify that it exists in at least one node's primary store
    # and that it appears in exactly k_factor-1 replicas.

    for node in chord_ring["nodes"]:
        node_ip = node['ip']
        node_port = node['port']
        resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
        if resp.status_code == 200:
            node_info = resp.json()
            # Check that each key is present has the expected number of replicas
            data = node_info["data_store"]
            for key in data:
                # Check its successors for the replicas
                resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
                node_info = resp.json()
                for i in range(k_factor - 1, 0, -1):
                    successor_ip = node_info["successor"]["ip"]
                    successor_port = node_info["successor"]["port"]
                    print("i", i)
                    print("Successor IP and Port")
                    print(successor_ip, successor_port)
                    resp = requests.get(f"http://{successor_ip}:{successor_port}/node_info")
                    if resp.status_code == 200:
                        node_info = resp.json()
                        replicas = node_info.get("replicas", {})
                        if key in replicas:
                            assert replicas[key][1] == i, (
                                f"Replica for key {key} in node {successor_ip}:{successor_port} has replication_count "
                                f"{replicas[key][1]}, expected {i}"
                            )

def test_replicas_upon_departure(chord_ring):
    # --- Simulate node departures ---
    # Nodes depart one by one, randomly chosen until only the first(bootstrap) node remains.
    global keys
    temp_chord = chord_ring["nodes"].copy()
    for i in range(len(chord_ring["nodes"])-1, 0, -1):
        departed_index = i
        departed_node = chord_ring["nodes"][departed_index]
        departed_ip = departed_node['ip']
        departed_port = departed_node['port']
        print(f"Node at index {departed_index} departing...")
        resp = requests.post(f"http://{departed_ip}:{departed_port}/depart")
        assert resp.status_code == 200, f"Failed to depart node at {departed_ip}:{departed_port}"
        print(f"Node at index {departed_index} departed.")
        time.sleep(3)  # Allow time for the departure to propagate
        # Visualize the ring after the departure
       # chord_graph = visualize_chord_ring(temp_chord)
        # Render and view the graph (this creates a file named chord_ring.gv.pdf and opens it)
      #  chord_graph.render('chord_ring.gv', view=True)
        temp_chord.pop(departed_index)
        # --- Verification of keys and replica counts after departure ---

        keys_and_replicas_verification(temp_chord)




# def test_rejoining_of_nodes(chord_ring):
#     # --- Simulate node rejoining ---
#     # Nodes rejoin one by one, randomly chosen until all nodes have rejoined the ring.
#     global keys
#     temp_chord = chord_ring["nodes"].copy()
#     for i in range(1, len(chord_ring["nodes"])):
#         rejoined_index = i
#         rejoined_node = chord_ring["nodes"][rejoined_index]
#         rejoined_ip = rejoined_node['ip']
#         rejoined_port = rejoined_node['port']
#         print(f"Node at index {rejoined_index} rejoining...")
#         resp = requests.post(f"http://{rejoined_ip}:{rejoined_port}/join", json={"ip": rejoined_ip, "port": rejoined_port})
#         assert resp.status_code == 200, f"Failed to rejoin node at {rejoined_ip}:{rejoined_port}"
#         print(f"Node at index {rejoined_index} rejoined.")
#         time.sleep(3)  # Allow time for the rejoining to propagate
#         # Visualize the ring after the rejoining
#         temp_chord.append(rejoined_node)
#         chord_graph = visualize_chord_ring(temp_chord)
#         # Render and view the graph (this creates a file named chord_ring.gv.pdf and opens it)
#         chord_graph.render('chord_ring.gv', view=True)
#
#         # --- Verification of keys and replica counts after rejoining ---
#         keys_and_replicas_verification(temp_chord)

def keys_and_replicas_verification(temp_chord):
    # For each departure iteration, verify that every key is present
    # in the ring after the departure.
    for key in keys:
        url_query = "http://127.0.0.1:5000/query"  # Query the bootstrap node
        response = requests.post(url_query, json={"key": key})

        assert response.status_code == 200
        # Check that the key is found in the ring after the departure
        assert response.json().get("value") is not None, f"Key '{key}' not found in the ring after departure"
    # --- Replica replication_count verification via ring traversal ---

    # For every key in a node's data_store (the primary copy), follow the successor chain
    # for the next k_factor-1 nodes and verify that each holds a replica with the expected replication_count.
    # CAREFUL! if the nodes remaining in the ring are less than k_factor-1
    if len(temp_chord) == 1:
        # If only the bootstrap node remains, break the loop
        # Check its self-replicas dict is empty
        node_ip = temp_chord[0]['ip']
        node_port = temp_chord[0]['port']
        resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
        assert resp.status_code == 200
        bnode_info = resp.json()
        assert bnode_info.get("replicas") == {}, "Self-replicas dict is not empty"

    for node in temp_chord:
        node_ip = node['ip']
        node_port = node['port']
        resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
        if resp.status_code == 200:
            node_info = resp.json()
            # Check that each key is present has the expected number of replicas
            data = node_info["data_store"]
            remaining_nodes = len(temp_chord)
            for key in data:
                node_info_succ = node_info
                if remaining_nodes < k_factor:
                    for j in range(k_factor - 1, k_factor - remaining_nodes, -1):
                        successor_ip = node_info_succ["successor"]["ip"]
                        successor_port = node_info_succ["successor"]["port"]
                        resp = requests.get(f"http://{successor_ip}:{successor_port}/node_info")
                        if resp.status_code == 200:
                            node_info_succ = resp.json()
                            replicas = node_info_succ.get("replicas", {})
                            if key in replicas:
                                assert replicas[key][1] == j, (
                                    f"Replica for key {key} in node {successor_ip}:{successor_port} has replication_count "
                                    f"{replicas[key][1]}, expected {j}"
                                )
                else:
                    for j in range(k_factor - 1, 0, -1):
                        successor_ip = node_info_succ["successor"]["ip"]
                        successor_port = node_info_succ["successor"]["port"]
                        resp = requests.get(f"http://{successor_ip}:{successor_port}/node_info")
                        if resp.status_code == 200:
                            node_info_succ = resp.json()
                            replicas = node_info.get("replicas", {})
                            if key in replicas:
                                assert replicas[key][1] == j, (
                                    f"Replica for key {key} in node {successor_ip}:{successor_port} has replication_count "
                                    f"{replicas[key][1]}, expected {j}"
                                )
        else:
            print(f"Error getting node_info from {node_ip}:{node_port}")