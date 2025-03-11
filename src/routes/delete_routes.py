from flask import Blueprint, request, jsonify, g
from helper_functions import *
delete_bp = Blueprint('delete', __name__)


@delete_bp.route('/delete', methods=['POST'])
def delete():
    node = g.node
    req = request.get_json()
    key = req.get("key")
    result = node.delete(key)
    return jsonify(result)

@delete_bp.route('/deleteReplicas', methods=['POST'])
def deleteReplicas():
    node = g.node
    req = request.get_json()
    key = req.get("key")
    replication_count = req.get("replication_count")
    result = node.deleteReplicas(key, replication_count)
    return jsonify(result)