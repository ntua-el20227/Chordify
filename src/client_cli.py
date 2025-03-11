import requests
import shlex
import threading
import os
from concurrent.futures import ThreadPoolExecutor

def print_help():
    help_text = """
Available commands:
  insert <key> <value>   - Insert a (key, value) pair into the DHT.
  delete <key>           - Delete the (key, value) pair for the key.
  query <key>            - Retrieve the value for the key (use "*" for all).
  overlay                - Display the Chord ring topology.
  depart                 - Instruct the node to gracefully leave the DHT (clearing its DHT info).
  file_launch            - Launches a file from a node
  file_parallel          - Launches in parallel different files to different nodes
  help                   - Display this help message.
  exit                   - Exit the client.
"""
    print(help_text)

def send_request(method, base_url, endpoint, data=None, params=None):
    url = f"{base_url}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, params=params, timeout=5)
        else:
            response = requests.post(url, json=data, timeout=5)
        return response.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}
    

def launch_file(i, node_ip, node_port, launch_type):
    base_url = f"http://{node_ip}:{node_port}"
    if launch_type == "insert":
        file_path = os.path.join("..", "data", "insert_" + str(i) + ".txt")
        with open(file_path, "r") as file:
            while True:
                line = file.readline()
                if not line:  # Break if end of file
                    break
                data = {"key": line.strip(), "value": f"{node_ip}:{node_port}"}
                resp = send_request("POST", base_url, "/insert", data=data)
                print(resp)
    elif launch_type == "query":
        file_path = os.path.join("..", "data", "query_" + str(i) + ".txt")
        with open(file_path, "r") as file:
            while True:
                line = file.readline()
                if not line:  # Break if end of file
                    break
                data = {"key": line.strip()}
                resp = send_request("POST", base_url, "/query", data=data)
                print(resp)
    elif launch_type == "request":
        file_path = os.path.join("..", "data", "requests_" + str(i) + ".txt")
        with open(file_path, "r") as file:
            while True:
                line = file.readline().strip()
                if not line:  # Break if end of file
                    break
                parts = line.split(", ")
                request_type = parts[0]
                key = parts[1]
                if request_type == "query":
                    data = {"key": key}
                    resp = send_request("POST", base_url, "/query", data=data)
                    print(resp)
                elif request_type == "insert":
                    value = parts[2]
                    data = {"key": key, "value": value}
                    resp = send_request("POST", base_url, "/insert", data=data)
                    print(resp)
    else:
        print("Available type of launch: insert, query, request")





def main():
    print("Chordify CLI Client (Flask-based)")
    print("Type 'help' to see available commands.")
    while True:
        try:
            line = input("Enter command: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting client.")
            break
        if not line:
            continue
        try:
            tokens = shlex.split(line)
        except Exception as e:
            print("Error parsing command:", e)
            continue
        
        cmd = tokens[0].lower()
        if cmd == "exit":
            break
        elif cmd == "help":
            print_help()
        elif cmd == "insert":
            if len(tokens) < 5:
                print("Usage: insert <key> <value> <node_ip> <node_port>")
                continue
            key = tokens[1]
            value = tokens[2]
            node_ip = tokens[3]
            node_port = tokens[4]
            data = {"key": key, "value": value}
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("POST", base_url, "/insert", data=data)
            print(resp)
        elif cmd == "delete":
            if len(tokens) < 4:
                print("Usage: delete <key> <node_ip> <node_port>")
                continue
            key = tokens[1]
            node_ip = tokens[2]
            node_port = tokens[3]
            data = {"key": key}
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("POST", base_url, "/delete", data=data)
            print(resp)
        elif cmd == "query":
            if len(tokens) < 4:
                print("Usage: query <key> <node_ip> <node_port>")
                continue
            key = tokens[1]
            node_ip = tokens[2]
            node_port = tokens[3]
            data = {"key": key}
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("POST", base_url, "/query", data=data)
            print(resp)
        elif cmd == "overlay":
            if len(tokens) < 3:
                print("Usage: overlay <node_ip> <node_port>")
                continue
            node_ip = tokens[1]
            node_port = tokens[2]
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("GET", base_url, "/overlay")
            print(resp)
        elif cmd == "depart":
            if len(tokens) < 3:
                print("Usage: depart <node_ip> <node_port>")
                continue
            node_ip = tokens[1]
            node_port = tokens[2]
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("POST", base_url, "/depart")
            print(resp)
        elif cmd == "node_info":
            if len(tokens) < 3:
                print("Usage: node_info <node_ip> <node_port>")
                continue
            node_ip = tokens[1]
            node_port = tokens[2]
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("GET", base_url, "/node_info")
            print(resp)
###################################### single launch ######################################            
        elif cmd == "file_launch":                     
            if len(tokens) < 4:
                print("Usage: file_launch <node_ip> <node_port> <type>")
                continue
            node_ip = tokens[1]
            node_port = tokens[2]
            launch_type = tokens[3]
            launch_file(0, node_ip, node_port, launch_type)       
###################################### parallel launch ######################################
        elif cmd == "file_parallel":                        
            if len(tokens) < 4:
                print("Usage: file_parallel <node_ip> <node_port> <type>")
                continue
            node_ip = tokens[1]
            node_port = tokens[2]
            launch_type = tokens[3]
            base_url = f"http://{node_ip}:{node_port}"
            resp = send_request("GET", base_url, "/overlay")
            node_list = [(node["ip"], node["port"]) for node in resp["overlay"]]

            with ThreadPoolExecutor(max_workers=len(node_list)) as executor:
                futures = []
                for i, (ip, port) in enumerate(node_list):
                    futures.append(executor.submit(launch_file, i, ip, port, launch_type))
                
                for future in futures:
                    future.result()
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
