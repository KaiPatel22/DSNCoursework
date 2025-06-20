This project contains my coursework for my Distributed Systems and Networks coursework.

The project was to create a distributed storage system for files using a TCP connection to start multiple Dstores which connect to a Controller.
The Controller can then recieve operations from multiple Clients such as STORE, REMOVE, LIST which would then communicate with the Dstore to perform these operations. 
This project used the concepts of multi-threading to ensure operations from Clients don't overwrite each other and networking to ensure ACKs were recieved and messages were sent to the correct ports. 
Mark Achieved: 83/100, 83%
