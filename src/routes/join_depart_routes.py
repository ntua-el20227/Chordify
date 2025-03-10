from flask import Blueprint, request, jsonify, g
from controllers.node import Node
from helper_functions import *
import threading
join_depart_bp = Blueprint('join_depart', __name__)


@join_depart_bp.route('/join', methods=['POST'])
def join():
    node = g.node
    req = request.get_json()
    new_ip = req.get("ip")
    new_port = req.get("port")
    result = node.join(new_ip, new_port)
    return jsonify(result)

@join_depart_bp.route('/depart', methods=['POST'])
def depart():
    node = g.node
    result = node.depart()
    threading.Thread(target=shutdown_server).start()
    return jsonify(result)

@join_depart_bp.route('/update_successor', methods=['POST'])
def update_successor():
    node = g.node
    req = request.get_json()
    new_successor = req.get("new_successor")
    result = node.update_successor(new_successor)
    return jsonify(result)

@join_depart_bp.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    node = g.node
    req = request.get_json()
    new_predecessor = req.get("new_predecessor")
    result = node.update_predecessor(new_predecessor)
    return jsonify(result)

@join_depart_bp.route('/overlay', methods=['GET'])
def overlay():
    node = g.node
    # 'visited_ids' is expected to be a list of integers passed as query parameters.
    visited = request.args.getlist("visited_ids", type=int)
    result = node.overlay(visited)
    return jsonify(result)

@join_depart_bp.route('/node_info',methods=['GET'])
def node_info():
    node = g.node
    return jsonify(node.get_node_info())

@join_depart_bp.route('/set_config', methods=['POST'])
def set_config():
    node = g.node
    req = request.get_json()
    consistency = req.get("consistency")
    k_factor = req.get("k_factor")
    # Update the node's configuration; ensure that k_factor is an integer
    if consistency:
        node.consistency = consistency
    if k_factor:
        node.k_factor = int(k_factor)
    return jsonify({"status": "success", "consistency": node.consistency, "k_factor": node.k_factor})

@join_depart_bp.route('/shutdown', methods=['POST'])
def shutdown():
    threading.Thread(target=shutdown_server).start()
    return jsonify({"status": "success"})


