from flask import Blueprint, request, jsonify, g
from controllers.node import Node
from helper_functions import *
insert_bp = Blueprint('insert', __name__)

@insert_bp.route('/insert', methods=['POST'])
def insert():
    node = g.node
    req = request.get_json()
    key = req.get("key")
    value = req.get("value")
    client_ip = req.get("client_ip")
    client_port = req.get("client_port")
    result = node.insert(key, value, client_ip, client_port)
    return jsonify(result)

@insert_bp.route('/insertReplicas', methods=['POST'])
def insertReplicas():
    node = g.node
    req = request.get_json()
    key = req.get("key")
    value = req.get("value")
    replication_count = req.get("replication_count")
    join_ = req.get("join")
    starting_node = req.get("starting_node")
    client_ip = req.get("client_ip")
    client_port = req.get("client_port")
    result = node.insertReplicas(key, value, replication_count, join_, starting_node, client_ip, client_port)
    return jsonify(result)
