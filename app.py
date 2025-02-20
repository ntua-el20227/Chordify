from flask import Flask, request, jsonify
import requests
import hashlib
import sys

app = Flask(__name__)

# --- Global Node State ---
node_ip = None
node_port = None
node_id = None
data_store = {}  # Local key-value store
successor = None
predecessor = None

# --- Helper Functions ---
def hash_function(key):
    """Compute SHA-1 hash of a key mod 2^16."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**16)

def in_interval(x, start, end):
    """Check if x is in the circular interval (start, end] in modulo 2^16."""
    if start < end:
        return start < x <= end
    return x > start or x <= end

# --- Chord DHT Operations ---
@app.route('/insert', methods=['POST'])
def insert():
    req = request.get_json()
    key, value = req.get("key"), req.get("value")
    key_hash = hash_function(key)

    if node_id == predecessor["node_id"] or in_interval(key_hash, predecessor["node_id"], node_id):
        data_store[key] = data_store.get(key, "") + value  # Concatenate value
        return jsonify({"status": "success", "message": f"Inserted '{key}' at node {node_id}"})
    
    return requests.post(f"http://{successor['ip']}:{successor['port']}/insert", json=req).json()

@app.route('/query', methods=['POST'])
def query():
    req = request.get_json()
    key = req.get("key")

    if key == "*":
        visited = req.get("visited", [])
        if node_id in visited:
            return jsonify({"status": "success", "data": {}})
        visited.append(node_id)
        result = data_store.copy()
        response = requests.post(f"http://{successor['ip']}:{successor['port']}/query", json={"key": "*", "visited": visited}).json()
        if response.get("status") == "success":
            result.update(response.get("data", {}))
        return jsonify({"status": "success", "data": result})
    
    key_hash = hash_function(key)
    if node_id == predecessor["node_id"] or in_interval(key_hash, predecessor["node_id"], node_id):
        return jsonify({"status": "success", "value": data_store.get(key, "Key not found")})

    return requests.post(f"http://{successor['ip']}:{successor['port']}/query", json=req).json()

@app.route('/delete', methods=['POST'])
def delete():
    req = request.get_json()
    key = req.get("key")
    key_hash = hash_function(key)

    if node_id == predecessor["node_id"] or in_interval(key_hash, predecessor["node_id"], node_id):
        data_store.pop(key, None)
        return jsonify({"status": "success", "message": f"Deleted '{key}' from node {node_id}"})

    return requests.post(f"http://{successor['ip']}:{successor['port']}/delete", json=req).json()

@app.route('/join', methods=['POST'])
def join():
    global predecessor, successor  # Use global references

    try:
        req = request.get_json()
        new_ip, new_port = req.get("ip"), req.get("port")
        new_node_id = hash_function(f"{new_ip}:{new_port}")

        # Case 1: If this is the only node in the network
        if node_id == predecessor["node_id"] and node_id == successor["node_id"]:
            predecessor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}
            successor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}
            return jsonify({
                "status": "success",
                "new_successor": {"ip": node_ip, "port": node_port, "node_id": node_id},
                "new_predecessor": {"ip": node_ip, "port": node_port, "node_id": node_id},
                "transferred_keys": {}
            })

        # Case 2: The new node belongs between predecessor and this node
        if in_interval(new_node_id, predecessor["node_id"], node_id):
            old_predecessor = predecessor.copy()
            predecessor = {"ip": new_ip, "port": new_port, "node_id": new_node_id}

            # Transfer keys that now belong to the new node
            keys_to_transfer = {k: v for k, v in data_store.items() if in_interval(hash_function(k), old_predecessor["node_id"], new_node_id)}
            for k in keys_to_transfer:
                del data_store[k]

            # Inform the previous node to update its successor to point to the new node
            requests.post(f"http://{old_predecessor['ip']}:{old_predecessor['port']}/update_successor",
                          json={"new_successor": {"ip": new_ip, "port": new_port, "node_id": new_node_id}})

            # Inform the new node about its successor and predecessor
            return jsonify({
                "status": "success",
                "new_successor": {"ip": node_ip, "port": node_port, "node_id": node_id},
                "new_predecessor": old_predecessor,
                "transferred_keys": keys_to_transfer
            })

        # Case 3: The new node does not belong between predecessor and this node,
        # so we forward the request to the correct node (successor)
        response = requests.post(f"http://{successor['ip']}:{successor['port']}/join", json=req)
        return response.json()

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
    
@app.route('/update_successor', methods=['POST'])
def update_successor():
    global successor
    req = request.get_json()
    new_successor = req.get("new_successor")
    successor = new_successor
    print(f"[UPDATE] Node {node_id} updated its successor to {new_successor['node_id']}")
    return jsonify({"status": "success", "message": "Successor updated"})

@app.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    global predecessor
    req = request.get_json()
    new_predecessor = req.get("new_predecessor")
    predecessor = new_predecessor
    print(f"[UPDATE] Node {node_id} updated its predecessor to {new_predecessor['node_id']}")
    return jsonify({"status": "success", "message": "Predecessor updated"})

@app.route('/depart', methods=['POST'])
def depart():
    global data_store, predecessor, successor

    # Inform neighbors to update their references
    update_succ = {"new_successor": successor}
    url_pred = f"http://{predecessor['ip']}:{predecessor['port']}/update_successor"
    requests.post(url_pred, json=update_succ)

    update_pred = {"new_predecessor": predecessor}
    url_succ = f"http://{successor['ip']}:{successor['port']}/update_predecessor"
    requests.post(url_succ, json=update_pred)

    # Transfer keys to the successor
    transfer = {"keys": data_store}
    url_transfer = f"http://{successor['ip']}:{successor['port']}/transfer_keys"
    requests.post(url_transfer, json=transfer)

    data_store.clear()
    successor = None
    predecessor = None
    print(f"[DEPART] Node {node_id} departed gracefully.")
    return jsonify({"status": "success", "message": f"Node {node_id} departed gracefully"})


@app.route('/overlay', methods=['GET'])
def overlay():
    visited = request.args.getlist("visited_ids", type=int)
    if node_id in visited:
        return jsonify({"status": "success", "overlay": []})
    visited.append(node_id)
    overlay_list = [{"node_id": node_id, "ip": node_ip, "port": node_port}]
    response = requests.get(f"http://{successor['ip']}:{successor['port']}/overlay", params={"visited_ids": visited}).json()
    if response.get("status") == "success":
        overlay_list.extend(response.get("overlay", []))
    return jsonify({"status": "success", "overlay": overlay_list})

# --- Node Initialization ---
def initialize_node():
    global node_ip, node_port, node_id, successor, predecessor

    if len(sys.argv) < 3:
        print("Usage: python app.py <IP> <PORT> [BOOTSTRAP_IP] [BOOTSTRAP_PORT]")
        sys.exit(1)

    node_ip, node_port = sys.argv[1], int(sys.argv[2])
    node_id = hash_function(f"{node_ip}:{node_port}")
    successor = predecessor = {"ip": node_ip, "port": node_port, "node_id": node_id}
    print(f"[START] Node {node_id} at {node_ip}:{node_port}")

    if len(sys.argv) == 5:
        bootstrap_ip, bootstrap_port = sys.argv[3], int(sys.argv[4])
        response = requests.post(f"http://{bootstrap_ip}:{bootstrap_port}/join", json={"ip": node_ip, "port": node_port}).json()
        if response.get("status") == "success":
            successor, predecessor = response["new_successor"], response["new_predecessor"]
            data_store.update(response.get("transferred_keys", {}))
            print(f"[JOINED] Successor: {successor['node_id']}, Predecessor: {predecessor['node_id']}")
        else:
            print("[JOIN FAILED]", response)

if __name__ == "__main__":
    initialize_node()
    app.run(host=node_ip, port=node_port)
