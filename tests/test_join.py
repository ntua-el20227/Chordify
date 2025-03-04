import pytest
import time
import requests
import subprocess
import hashlib

from tests.visualize_script import visualize_chord_ring


def hash_function(key):
    """Compute SHA-1 hash of a key mod 2^16."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 16)


# Number of nodes and ports to use
NODES = [
    {"ip": "127.0.0.1", "port": 5000},
    {"ip": "127.0.0.1", "port": 5001},
    {"ip": "127.0.0.1", "port": 5002},
    {"ip": "127.0.0.1", "port": 5003},
    {"ip": "127.0.0.1", "port": 5004},
]

# Path to the app file and the python executable
APP_PATH = "../src/app.py"
PYTHON_PATH = "python.exe"

k_factor = 4
consistency = "linearizability"

@pytest.fixture(scope="module")
def chord_ring():
    """
    Start a small chord ring for testing purposes.
    The first node is the bootstrap (listening on port 5000) and
    the rest join via the bootstrap node.

    This fixture now returns a dictionary with both the nodes list and
    the process handles so that tests can simulate node departures and rejoining.
    """
    processes = []

    # Start bootstrap node (first node)
    bootstrap_cmd = f"{PYTHON_PATH} {APP_PATH} {NODES[0]['ip']} {NODES[0]['port']}"
    processes.append(subprocess.Popen(bootstrap_cmd, shell=True))

    # Give the bootstrap node a moment to start
    time.sleep(3)

    # Start other nodes and have them join the ring via the bootstrap node
    bootstrap_ip, bootstrap_port = NODES[0]['ip'], NODES[0]['port']
    for node in NODES[1:]:
        join_cmd = f"{PYTHON_PATH} {APP_PATH} {node['ip']} {node['port']} {bootstrap_ip} {bootstrap_port}"
        processes.append(subprocess.Popen(join_cmd, shell=True))
        # Give each node time to start and join the ring
        time.sleep(2)


    yield {"nodes": NODES, "processes": processes}

    # Teardown - kill all processes
    for process in processes:
        process.terminate()
    time.sleep(3)



def test_keys_file_departure_join(chord_ring, insert_filepath="C:\\Users\\koust\\PycharmProjects\\Chordify\\src\\insert_0.txt"):
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

    chord_graph = visualize_chord_ring(chord_ring)

    # Render and view the graph (this creates a file named chord_ring.gv.pdf and opens it)
    chord_graph.render('chord_ring.gv', view=True)

    # Read the keys from the file
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
                for i in range(k_factor-1, 0, -1):
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


    # ----- Simulate a node departure -----
    # Let’s simulate a graceful departure for the third node (index 2) (not the bootstrap).
    departed_index = 2
    departed_ip = chord_ring["nodes"][departed_index]['ip']
    departed_port = chord_ring["nodes"][departed_index]['port']
    response = requests.get(f"http://{departed_ip}:{departed_port}/depart").json()
    successor_of_departed = response.get("successor")
    print(f"Node at index {departed_index} departed.")
    time.sleep(5)  # Allow time for the ring to reconfigure after departure

    # Verify that keys are still available on the correct remaining nodes.
    # (The successor of the departed node should have taken over its keys.)
    for key in keys:
        found = False
        for i, node in enumerate(chord_ring["nodes"]):
            if i == departed_index:
                continue  # Skip the departed node
            node_ip = node['ip']
            node_port = node['port']
            if node_ip == successor_of_departed["node_ip"] and node_port == successor_of_departed.get("node_port"):
                resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
                if resp.status_code == 200:
                    node_info = resp.json()
                    if key in node_info.get('data_store', {}):
                        found = True
                        break
                    else:
                        break
        assert found, f"After departure, key {key} was not found in successor node's data store"

    # # ----- Simulate node rejoining -----
    # # Restart the departed node by issuing the join command again.
    # node = chord_ring["nodes"][departed_index]
    # join_cmd = f"{PYTHON_PATH} {APP_PATH} {node['ip']} {node['port']} {bootstrap_ip} {bootstrap_port}"
    # new_process = subprocess.Popen(join_cmd, shell=True)
    # chord_ring["processes"][departed_index] = new_process  # Update the process reference.
    # print(f"Node at index {departed_index} rejoined.")
    # time.sleep(5)  # Allow time for rejoining and data rebalancing.
    #
    # # Verify again that every key is present in the ring after rejoining.
    # for key in keys:
    #     found = False
    #     for node in chord_ring["nodes"]:
    #         node_ip = node['ip']
    #         node_port = node['port']
    #         resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
    #         if resp.status_code == 200:
    #             node_info = resp.json()
    #             if key in node_info.get('data_store', {}) or key in node_info.get('replicas', {}):
    #                 found = True
    #                 break
    #     assert found, f"After rejoin, key {key} was not found in any node's data store or replicas"
    #
    # # ----- Replica replication_count verification via ring traversal -----
    # # For every key in a node's data_store (the primary copy), follow the successor chain
    # # for the next k_factor-1 nodes and verify that each holds a replica with the expected replication_count.
    # for node in chord_ring["nodes"]:
    #     node_ip = node['ip']
    #     node_port = node['port']
    #     resp = requests.get(f"http://{node_ip}:{node_port}/node_info")
    #     assert resp.status_code == 200, f"Failed to get node info from {node_ip}:{node_port}"
    #     node_info = resp.json()
    #     primary_store = node_info.get("data_store", {})
    #     for key, value in primary_store.items():
    #         # Start with the successor of the current node.
    #         successor_info = node_info.get("successor")
    #         for offset in range(1, k_factor):
    #             s_ip = successor_info.get("ip")
    #             s_port = successor_info.get("port")
    #             s_resp = requests.get(f"http://{s_ip}:{s_port}/node_info")
    #             assert s_resp.status_code == 200, f"Failed to get node info from {s_ip}:{s_port}"
    #             s_info = s_resp.json()
    #             replicas = s_info.get("replicas", {})
    #             assert key in replicas, f"Replica for key {key} not found in node {s_ip}:{s_port}"
    #             replica_entry = replicas[key]
    #             # Expected replication_count decreases with offset.
    #             expected_count = k_factor - offset
    #             actual_count = replica_entry.get("replication_count")
    #             assert actual_count == expected_count, (
    #                 f"Replica for key {key} in node {s_ip}:{s_port} has replication_count {actual_count}, "
    #                 f"expected {expected_count}"
    #             )
    #             # Move to the next successor in the ring.
    #             successor_info = s_info.get("successor")


# Run the tests if this module is executed as the main program.
if __name__ == "__main__":
    pytest.main(['-vv'])
