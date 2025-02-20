from flask import Blueprint, request, jsonify
from app.dht.node import Node  # Assuming Node contains your DHT logic

# Create a blueprint for the API routes
api = Blueprint('api', __name__)

# Instantiate your DHT node
node = Node()

@api.route('/', methods=['GET'])
def index():
    """Health check endpoint."""
    return jsonify({"message": "Chordify API is running."})

@api.route('/insert', methods=['POST'])
def insert():
    """
    Insert a new <key, value> pair or update an existing key.
    Expected JSON payload:
      {
          "key": "song_name",
          "value": "ip_address"
      }
    """
    data = request.get_json()
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({"error": "Missing key or value in request"}), 400

    # The Node.insert method should handle SHA1 hashing, routing, and replication
    result = node.insert(data['key'], data['value'])
    return jsonify(result), 200

@api.route('/query', methods=['GET'])
def query():
    """
    Query a key in the DHT.
    Pass the key as a query parameter: /query?key=song_name
    Use "*" as key to return all <key, value> pairs.
    """
    key = request.args.get('key')
    if not key:
        return jsonify({"error": "Missing key parameter"}), 400

    result = node.query(key)
    return jsonify(result), 200

@api.route('/delete', methods=['POST'])
def delete():
    """
    Delete a <key, value> pair.
    Expected JSON payload:
      {
          "key": "song_name"
      }
    """
    data = request.get_json()
    if not data or 'key' not in data:
        return jsonify({"error": "Missing key in request"}), 400

    result = node.delete(data['key'])
    return jsonify(result), 200

@api.route('/join', methods=['POST'])
def join():
    """
    Endpoint for a new node to join the DHT.
    Expected JSON payload:
      {
          "ip": "node_ip_address",
          "port": "listening_port"
      }
    """
    data = request.get_json()
    if not data or 'ip' not in data or 'port' not in data:
        return jsonify({"error": "Missing ip or port in request"}), 400

    result = node.join(data['ip'], data['port'])
    return jsonify(result), 200

@api.route('/depart', methods=['POST'])
def depart():
    """
    Endpoint for a node to gracefully leave the DHT.
    Expected JSON payload:
      {
          "ip": "node_ip_address",
          "port": "listening_port"
      }
    """
    data = request.get_json()
    if not data or 'ip' not in data or 'port' not in data:
        return jsonify({"error": "Missing ip or port in request"}), 400

    result = node.depart(data['ip'], data['port'])
    return jsonify(result), 200

# Optionally, add more endpoints (e.g., replication trigger, status updates, etc.)
