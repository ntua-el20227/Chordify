# Chordify: A P2P Chord-DHT application

  [![License](https://img.shields.io/static/v1?label=License&message=MIT&color=blue&?style=plastic&logo=appveyor)](https://opensource.org/license/MIT)



## Table Of Content

- [Description](#description)

- [Installation](#installation)


- [Tests](#tests)
- [GitHub](#github)

- [License](#license)




![GitHub repo size](https://img.shields.io/github/repo-size/ntua-el20227/Chordify?style=plastic)

  ![GitHub top language](https://img.shields.io/github/languages/top/ntua-el20227/Chordify?style=plastic)



## Description

  As part of the semester project in our Distributed Systems class we created an application that is using the Chord protocol-algorithm for a Peer-to-Peer network of nodes. Each node was in a different ip address/port of the cluster of VMs that were running on the AWS cloud provider. The data was (key,value) pairs with song titles as keys and values with little essence for the goals of the project. 












## Installation

The project has been developed using the Flask application framework and Python 3.12. After cloning the project you can use "pip install -r requirements.txt" for the installation of the required dependecies. Based on the instructions given we used a bootstrap node as a SPOF(Unofortunately!) to manage any joins and departs of other nodes. As a result, it is IMPORTANT to first deploy the bootstrap node with "python3 app.py <bootstrap-ip> <bootstrap-port>" and then for every other node deploymentpython3 app.py <node-ip> <node-port> <bootstrap-ip> <bootstrap-port>". Finally, for the client server use "python3 client_cli.py <ip you want it to run on>"





Chordify: A P2P Chord-DHT application is built with the following tools and libraries: <ul><li>Python3</li> <li>Flask</li> <li>AWS</li></ul>











## Tests
 
In the ./tests directory there is a test.py script that uses the pytest library to test some basic functionalities of the application. Run it as a normal python script when the nodes are down.






## GitHub

<a href="https://github.com/ntua-el20227"><strong>ntua-el20227</a></strong>



<p>Visit my website: <strong><a href="https://github.com/ntua-el20227"></a></strong></p>








## License

[![License](https://img.shields.io/static/v1?label=Licence&message=MIT&color=blue)](https://opensource.org/license/MIT)


