#!/usr/bin/env python3
import requests
import json
import shlex

def print_help():
    help_text = """
Available commands:
  insert <key> <value>   - Insert a (key, value) pair into the DHT.
  delete <key>           - Delete the (key, value) pair for the key.
  query <key>            - Retrieve the value for the key (use "*" for all).
  overlay                - Display the Chord ring topology.
  depart                 - Instruct the node to gracefully leave the DHT (clearing its DHT info).
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

def main():
    print("Chordify CLI Client (Flask-based)")
    node_ip = input("Enter node IP: ").strip()
    try:
        node_port = int(input("Enter node Port: ").strip())
    except ValueError:
        print("Port must be an integer.")
        return
    base_url = f"http://{node_ip}:{node_port}"
    
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
            if len(tokens) < 3:
                print("Usage: insert <key> <value>")
                continue
            key = tokens[1]
            value = tokens[2]
            data = {"key": key, "value": value}
            resp = send_request("POST", base_url, "/insert", data=data)
            print(resp)
        elif cmd == "delete":
            if len(tokens) < 2:
                print("Usage: delete <key>")
                continue
            key = tokens[1]
            data = {"key": key}
            resp = send_request("POST", base_url, "/delete", data=data)
            print(resp)
        elif cmd == "query":
            if len(tokens) < 2:
                print("Usage: query <key>")
                continue
            key = tokens[1]
            data = {"key": key}
            resp = send_request("POST", base_url, "/query", data=data)
            print(resp)
        elif cmd == "overlay":
            resp = send_request("GET", base_url, "/overlay")
            print(resp)
        elif cmd == "depart":
            resp = send_request("POST", base_url, "/depart")
            print(resp)
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
