from ensurepip import bootstrap

from flask import Flask, request, jsonify
import requests
import hashlib
import sys
from node import Node

app = Flask(__name__)

# --- Chord DHT Operations ---
@app.route('/insert', methods=['POST'])
def insert():
    req = request.get_json()
    key = req.get("key")
    value = req.get("value")
    result = node.insert(key, value)
    return jsonify(result)


@app.route('/query', methods=['POST'])
def query():
    req = request.get_json()
    key = req.get("key")
    result = node.query(key)
    return jsonify(result)


@app.route('/delete', methods=['POST'])
def delete():
    req = request.get_json()
    key = req.get("key")
    result = node.delete(key)
    return jsonify(result)


@app.route('/join', methods=['POST'])
def join():
    req = request.get_json()
    new_ip = req.get("ip")
    new_port = req.get("port")
    result = node.join(new_ip, new_port)
    return jsonify(result)


@app.route('/update_successor', methods=['POST'])
def update_successor():
    req = request.get_json()
    new_successor = req.get("new_successor")
    result = node.update_successor(new_successor)
    return jsonify(result)


@app.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    req = request.get_json()
    new_predecessor = req.get("new_predecessor")
    result = node.update_predecessor(new_predecessor)
    return jsonify(result)


@app.route('/depart', methods=['POST'])
def depart():
    result = node.depart()
    return jsonify(result)


@app.route('/overlay', methods=['GET'])
def overlay():
    # 'visited_ids' is expected to be a list of integers passed as query parameters.
    visited = request.args.getlist("visited_ids", type=int)
    result = node.overlay(visited)
    return jsonify(result)

# --- Node Initialization ---
def initialize_node():

    if len(sys.argv) < 3:
        print("Usage: python app.py <IP> <PORT> [BOOTSTRAP_IP] [BOOTSTRAP_PORT] [consistency] [kfactor]")
        sys.exit(1)

    node_ip, node_port = sys.argv[1], int(sys.argv[2])
    # Ask for consistency
    if len(sys.argv) == 3:
        consistency = input("Consistency (linearizability or eventual): ").strip().lower()
        k_factor = input("Kfactor: ")
        if consistency not in ["linearizability", "eventual"]:
            print("Invalid consistency type. Please choose 'linearizability' or 'eventual'.")
            sys.exit(1)
        return Node(ip=node_ip, port=node_port, consistency=consistency, k_factor=k_factor)

    elif len(sys.argv) == 5:
        bootstrap_ip, bootstrap_port = sys.argv[3], int(sys.argv[4])
        response = requests.post(f"http://{bootstrap_ip}:{bootstrap_port}/join", json={"ip": node_ip, "port": node_port}).json()
        if response.get("status") == "success":
            successor, predecessor = response["new_successor"], response["new_predecessor"]
            consistency = response.get("consistency")
            k_factor = response.get("k_factor")
            #data_store.update(response.get("transferred_keys", {}))
            print(f"[JOINED] Successor: {successor['node_id']}, Predecessor: {predecessor['node_id']}")
        else:
            print("[JOIN FAILED]", response)
        return Node(ip=node_ip, port=node_port, consistency=consistency, k_factor=k_factor, successor=successor, predecessor=predecessor)

if __name__ == "__main__":
    # Initialize the node (with bootstrap parameters as required).
    node = initialize_node()
    app.run(host=node.ip, port=node.port)
