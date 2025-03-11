import sys
from controllers.node import Node
from helper_functions import *
import threading
from flask import Flask
from routes._init_ import register_routes


app = Flask(__name__)

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
    
    register_routes(app, node)

    threading.Thread(target=replica_handler, args=(node,), daemon=True).start()
    # Make sure every process on this port is killed before starting the server
    app.run(host=node.ip, port=node.port)