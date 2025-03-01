import pytest
import time
import requests
import subprocess

# Number of nodes and ports to use
NODES = [
    {"ip": "127.0.0.1", "port": 5000},
    {"ip": "127.0.0.1", "port": 5001},
    {"ip": "127.0.0.1", "port": 5002},
    {"ip": "127.0.0.1", "port": 5003},
    {"ip": "127.0.0.1", "port": 5004},
]

# Path to the app file
APP_PATH = "../src/app.py"


@pytest.fixture(scope="module")
def chord_ring():
    """
    Start a small chord ring (5 nodes for example) for testing purposes.
    Nodes 5001+ join via the bootstrap node at 5000.
    """

    processes = []

    # Start bootstrap node (first node)
    bootstrap_cmd = f"python {APP_PATH} {NODES[0]['ip']} {NODES[0]['port']}"
    processes.append(subprocess.Popen(bootstrap_cmd, shell=True))

    # Give it a moment to spin up
    time.sleep(3)

    # Simulate user input for consistency and k_factor for bootstrap node
    requests.post(f"http://{NODES[0]['ip']}:{NODES[0]['port']}/set_config", json={
        "consistency": "eventual",
        "k_factor": 2
    })

    # Start other nodes and have them join the ring
    bootstrap_ip, bootstrap_port = NODES[0]['ip'], NODES[0]['port']

    for node in NODES[1:]:
        join_cmd = f"python {APP_PATH} {node['ip']} {node['port']} {bootstrap_ip} {bootstrap_port}"
        processes.append(subprocess.Popen(join_cmd, shell=True))

        # Give each node a bit of time to start and join
        time.sleep(2)

    yield NODES  # This lets the tests access the node info if needed

    # Teardown - kill all processes
    for process in processes:
        process.terminate()

    time.sleep(3)


def test_transferred_keys(chord_ring):
    """
    Example test to check if keys are transferred correctly when a new node joins.
    """
    node_ip, node_port = NODES[0]['ip'], NODES[0]['port']
    response = requests.get(f"http://{node_ip}:{node_port}/debug_state")

    assert response.status_code == 200
    state = response.json()

    # Check that transferred keys exist (this would depend on your actual logic)
    assert 'transferred_keys' in state, "Transferred keys should exist in debug state"
    print("Transferred keys:", state['transferred_keys'])


def test_replicas_of_transferred_keys(chord_ring):
    """
    Check if replicas are correctly created when a new node joins.
    """
    node_ip, node_port = NODES[0]['ip'], NODES[0]['port']
    response = requests.get(f"http://{node_ip}:{node_port}/debug_state")

    assert response.status_code == 200
    state = response.json()

    # Check for replicas (this depends on your actual replica management)
    assert 'replicas' in state, "Replicas should exist in debug state"
    print("Replicas:", state['replicas'])


def test_shifted_replicas(chord_ring):
    """
    Check that when a new node joins, the replicas shift correctly.
    """
    # This test is somewhat speculative since you haven't shown the whole logic.
    # Idea: When a node joins, some keys should shift, including replicas.
    node_ip, node_port = NODES[0]['ip'], NODES[0]['port']
    response = requests.get(f"http://{node_ip}:{node_port}/debug_state")

    assert response.status_code == 200
    state = response.json()

    # This check could depend on how your system tracks shifted replicas
    assert 'shifted_replicas' in state, "Shifted replicas should exist in debug state"
    print("Shifted replicas:", state['shifted_replicas'])

def test_insert_key(chord_ring):
    """
    Test inserting a key into the ring.
    """
    node_ip, node_port = NODES[0]['ip'], NODES[0]['port']
    key = "test_key"
    value = "test_value"

    """
        def get_node_info(self):
        
        #Return the node's information (ID, IP, port, successor, predecessor).
        return {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "successor": self.successor,
            "predecessor": self.predecessor,
            "data_store": self.data_store,
            "replicas": self.replicas
        }
    """
    # Insert the key
    response = requests.post(f"http://{node_ip}:{node_port}/insert", json={"key": key, "value": value})
    assert response.status_code == 200
    print(response.json())
    # Check if the key exists
    response = requests.get(f"http://{node_ip}:{node_port}/query")
    assert response.status_code == 200
    assert response.json()['value'] == value

# For each node make sure that k_factor -1 next nodes have each data_store key of current node as replica with the same value
def test_replicas(chord_ring):
    """
    Test that replicas are correctly maintained in the ring.
    """
    for i, node in enumerate(NODES):
        node_ip, node_port = node['ip'], node['port']
        response = requests.get(f"http://{node_ip}:{node_port}/node_info")
        assert response.status_code == 200
        node_info = response.json()
        print(node_info)
        # Check replicas for each key in the data store
        for key, value in node_info['data_store'].items():
            # Check the replicas for each key
            for replica_node in NODES[i+1:i+node_info['k_factor']]:
                replica_ip, replica_port = replica_node['ip'], replica_node['port']
                replica_response = requests.get(f"http://{replica_ip}:{replica_port}/node_info")
                replica_node_info = replica_response.json()
                assert replica_response.status_code == 200
                assert key in replica_node_info['replicas']
                assert replica_node_info['replicas'][key] == value


# Run the tests
if __name__ == "__main__":
    pytest.main(['-vv'])