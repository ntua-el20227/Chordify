from flask import Blueprint, request, jsonify, g
from controllers.node import Node
from helper_functions import *
transfer_keys_bp = Blueprint('transfer_keys', __name__) 


@transfer_keys_bp.route('/transfer_keys', methods=['POST'])
def transfer_keys():
    node = g.node
    req = request.get_json()
    keys = req.get("keys")
    result = node.transfer_keys(keys)
    return jsonify(result)

@transfer_keys_bp.route('/transfer_replicas', methods=['POST'])
def transfer_replicas():
    node = g.node
    req = request.get_json()
    replicas = req.get("replicas")
    result = node.transfer_replicas(replicas)
    return jsonify(result)

@transfer_keys_bp.route('/generate_replicas', methods=['POST'])
def generate_replicas():
    node = g.node
    req = request.get_json()
    key = req.get("keys")
    result = node.generate_replicas(key)
    return jsonify(result)

@transfer_keys_bp.route('/remove_transferred_replicas', methods=['POST'])
def remove_transferred_replicas():
    node = g.node
    req = request.get_json()
    replicas = req.get("keys")
    result = node.remove_transferred_replicas(replicas)
    return jsonify(result)

@transfer_keys_bp.route('/shift_replicas', methods=['POST'])
def shift_replicas():
    node = g.node
    req = request.get_json()
    data = req.get("keys")
    replicas = req.get("replicas")
    starting_node = req.get("starting_node")
    result = node.shift_replicas(data, replicas, starting_node)
    return jsonify(result)

@transfer_keys_bp.route('/updateReplicas', methods=['POST'])
def updateReplicas():
    node = g.node
    req = request.get_json()
    replicas = req.get("replicas")
    new_node_id = req.get("new_node_id")
    result = node.updateReplicas(replicas, new_node_id)
    return jsonify(result)

