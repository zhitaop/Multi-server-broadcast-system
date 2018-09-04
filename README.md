# Multi-server-broadcast-system

This is a multi-server system for broadcasting messages between clients inplemented in Java.
Communications are via TCP sockets and the messages are sent as JSON objects. 
Features include:
* Clients can broadcast messages to all other clients connnected at the time.
* Allow clients to register a username and secret  pair to act as authentication, or login as anonymous
* Load balancing clients among the servers using redirection mechanism
* Servers will try to connect to other servers in the sytem if server it's originally connected to crashes, to maintain system integrity and availability for the clients
