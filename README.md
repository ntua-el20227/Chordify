# Chordify: A P2P Chord-DHT Application

[![License: MIT](https://img.shields.io/static/v1?label=License&message=MIT&color=blue&style=plastic)](https://opensource.org/license/MIT)  
[![GitHub repo size](https://img.shields.io/github/repo-size/ntua-el20227/Chordify?style=plastic)](https://github.com/ntua-el20227/Chordify)  
[![GitHub top language](https://img.shields.io/github/languages/top/ntua-el20227/Chordify?style=plastic)](https://github.com/ntua-el20227/Chordify)  

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://www.python.org)  
[![Flask](https://img.shields.io/badge/Flask-2.0%2B-blue?logo=flask)](https://flask.palletsprojects.com/)  
[![AWS](https://img.shields.io/badge/AWS-Amazon%20Web%20Services-orange?logo=amazon-aws)](https://aws.amazon.com)

---

## Table of Contents

- [Description](#description)
- [Installation](#installation)
- [Tests](#tests)
- [References](#references)
- [License](#license)

---

## Description

Chordify is a peer-to-peer Chord-DHT application developed as part of a Distributed Systems semester project. The system implements the Chord protocol to manage a distributed hash table (DHT) across nodes deployed on AWS VMs. Each node operates on a unique IP/port combination, storing (key, value) pairs where keys are song titles.

---

## Installation

Chordify is built using **Python 3.12** and the **Flask** web framework. Follow the steps below to set up your environment:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/ntua-el20227/Chordify.git
   cd Chordify
## Installation

2. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Deploy the Bootstrap Node:**

   ```bash
   python3 app.py <bootstrap-ip> <bootstrap-port>
   ```

4. **Deploy Additional Nodes:**

   ```bash
   python3 app.py <node-ip> <node-port> <bootstrap-ip> <bootstrap-port>
   ```

5. **Run the Client CLI:**

   ```bash
   python3 client_cli.py <desired-ip>
   ```

---

## Tests

A basic test suite using the `pytest` library is available in the `./tests` directory. Ensure that all nodes are down before running tests:

```bash
python3 tests/test.py
```


---
## References

Chord: A Scalable Peer-to-peer Lookup Protocol
for Internet Applications
Ion Stoica†, Robert Morris‡, David Liben-Nowell‡, David R. Karger‡, M. Frans Kaashoek‡, Frank Dabek‡, Hari Balakrishnan‡

## License

Chordify is licensed under the [MIT License](https://opensource.org/license/MIT).
