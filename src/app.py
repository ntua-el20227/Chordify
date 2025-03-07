from flask import Flask, request, jsonify
import requests
import sys
from node import Node
from helper_functions import *
import threading


app = Flask(__name__)

# --- Chord DHT Operations ---
@app.route('/shutdown', methods=['POST'])
def shutdown():
    threading.Thread(target=shutdown_server).start()
    return jsonify({"status": "success"})
@app.route('/insert', methods=['POST'])
def insert():
    req = request.get_json()
    key = req.get("key")
    value = req.get("value")
    result = node.insert(key, value)
    return jsonify(result)

@app.route('/insertReplicas', methods=['POST'])
def insertReplicas():
    req = request.get_json()
    key = req.get("key")
    value = req.get("value")
    replication_count = req.get("replication_count")
    join_ = req.get("join")
    starting_node = req.get("starting_node")
    result = node.insertReplicas(key, value, replication_count, join_, starting_node)
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

@app.route('/deleteReplicas', methods=['POST'])
def deleteReplicas():
    req = request.get_json()
    key = req.get("key")
    replication_count = req.get("replication_count")
    result = node.deleteReplicas(key, replication_count)
    return jsonify(result)

@app.route('/join', methods=['POST'])
def join():
    req = request.get_json()
    new_ip = req.get("ip")
    new_port = req.get("port")
    result = node.join(new_ip, new_port)
    return jsonify(result)
@app.route('/transfer_replicas', methods=['POST'])
def transfer_replicas():
    req = request.get_json()
    replicas = req.get("replicas")
    result = node.transfer_replicas(replicas)
    return jsonify(result)
@app.route('/generate_replicas', methods=['POST'])
def generate_replicas():
    req = request.get_json()
    key = req.get("keys")
    result = node.generate_replicas(key)
    return jsonify(result)

@app.route('/updateReplicas', methods=['POST'])
def updateReplicas():
    req = request.get_json()
    replicas = req.get("replicas")
    new_node_id = req.get("new_node_id")
    result = node.updateReplicas(replicas, new_node_id)
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
    threading.Thread(target=shutdown_server).start()
    return jsonify(result)

@app.route('/remove_transferred_replicas', methods=['POST'])
def remove_transferred_replicas():
    req = request.get_json()
    replicas = req.get("keys")
    result = node.remove_transferred_replicas(replicas)
    return jsonify(result)

@app.route('/transfer_keys', methods=['POST'])
def transfer_keys():
    req = request.get_json()
    keys = req.get("keys")
    result = node.transfer_keys(keys)
    return jsonify(result)

@app.route('/shift_replicas', methods=['POST'])
def shift_replicas():
    req = request.get_json()
    data = req.get("keys")
    replicas = req.get("replicas")
    starting_node = req.get("starting_node")
    result = node.shift_replicas(data, replicas, starting_node)
    return jsonify(result)

@app.route('/overlay', methods=['GET'])
def overlay():
    # 'visited_ids' is expected to be a list of integers passed as query parameters.
    visited = request.args.getlist("visited_ids", type=int)
    result = node.overlay(visited)
    return jsonify(result)

@app.route('/node_info',methods=['GET'])
def node_info():
    return jsonify(node.get_node_info())

@app.route('/set_config', methods=['POST'])
def set_config():
    req = request.get_json()
    consistency = req.get("consistency")
    k_factor = req.get("k_factor")
    # Update the node's configuration; ensure that k_factor is an integer
    if consistency:
        node.consistency = consistency
    if k_factor:
        node.k_factor = int(k_factor)
    return jsonify({"status": "success", "consistency": node.consistency, "k_factor": node.k_factor})



def initialize_node():
    if len(sys.argv) < 3:
        print("Usage: python app.py <IP> <PORT> [BOOTSTRAP_IP] [BOOTSTRAP_PORT] [consistency] [kfactor]")
        sys.exit(1)

    node_ip, node_port = sys.argv[1], int(sys.argv[2])

    # If only IP and PORT are provided, ask for configuration interactively
    if len(sys.argv) == 3:
        while True:
            try:
                consistency = input("Consistency (linearizability(l) or eventual(e)): ").strip().lower()
                if consistency == "l":
                    consistency = "linearizability"
                elif consistency == "e":
                    consistency = "eventual"
                else:
                    print("Invalid consistency type. Please choose 'linearizability' or 'eventual'.")
                    continue  # prompt again

                k_factor = int(input("Kfactor: "))
                if k_factor < 1 or k_factor > 10:
                    print("Invalid kfactor. Please enter a positive integer between 1 and 10.")
                    continue  # prompt again
                break  # valid inputs provided, exit loop
            except ValueError:
                print("Invalid input. Please ensure you enter the correct value for kfactor.")
            except EOFError:
                # Non-interactive mode: set default values
                print("Non-interactive mode detected: setting default values.")
                consistency = "linearizability"
                k_factor = 4
                break

        return Node(ip=node_ip, port=node_port, consistency=consistency, k_factor=k_factor)

    elif len(sys.argv) == 5:
        bootstrap_ip, bootstrap_port = sys.argv[3], int(sys.argv[4])
        response = requests.post(f"http://{bootstrap_ip}:{bootstrap_port}/join",
                                 json={"ip": node_ip, "port": node_port})
        print(response)
        response = response.json()
        if response.get("status") == "success":
            successor, predecessor = response["new_successor"], response["new_predecessor"]
            consistency = response.get("consistency")
            k_factor = response.get("k_factor")
            data_store = response.get("transferred_keys", {})
            replicas = response.get("transferred_replicas", {})
            print(f"[JOINED] Successor: {successor['node_id']}, Predecessor: {predecessor['node_id']},"
                  f" Consistency: {consistency}, K-factor: {k_factor},"
                  f" Data Store: {data_store}, Replicas: {replicas}")
        else:
            print("[JOIN FAILED]", response)
        return Node(ip=node_ip, port=node_port, consistency=consistency, k_factor=k_factor,
                    successor=successor, predecessor=predecessor, data_store=data_store, replicas=replicas)

if __name__ == "__main__":
    # Initialize the node (with bootstrap parameters as required).
    node = initialize_node()

    threading.Thread(target=replica_handler, args=(node,), daemon=True).start()
    # Make sure every process on this port is killed before starting the server
    app.run(host=node.ip, port=node.port)
