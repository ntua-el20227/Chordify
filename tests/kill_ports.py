import subprocess
import re


def kill_ports(ports):
    # Get the full netstat output (as text)
    netstat_output = subprocess.check_output(["netstat", "-ano"], text=True)

    # For each port in our list, search for lines that match a local address of 127.0.0.1:<port> in LISTENING state.
    for port in ports:
        print(f"Searching for processes on port {port}...")
        # This regex matches a line like:
        # "  TCP    127.0.0.1:5000         0.0.0.0:0              LISTENING       34880"
        pattern = re.compile(rf"\s*TCP\s+127\.0\.0\.1:{port}\s+\S+\s+LISTENING\s+(\d+)", re.IGNORECASE)
        pids = pattern.findall(netstat_output)
        if not pids:
            print(f"  No processes found on port {port}.")
        else:
            for pid in pids:
                print(f"  -> Killing PID {pid} on port {port}")
                # /T kills the entire process tree, /F forces the kill
                subprocess.run(["taskkill", "/PID", pid, "/F", "/T"], check=True)


if __name__ == "__main__":
    # List of ports to check
    target_ports = [5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007]
    kill_ports(target_ports)
