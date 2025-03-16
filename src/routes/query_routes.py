from flask import Blueprint, request, jsonify, g
from helper_functions import *
query_bp = Blueprint('query', __name__)

@query_bp.route('/query', methods=['POST'])
def query():
    node = g.node
    req = request.get_json()
    key = req.get("key")
    client_ip = req.get("client_ip")
    client_port = req.get("client_port")
    result = node.query(key, client_ip, client_port)
    return jsonify(result)
