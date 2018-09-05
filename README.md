# Multi-server-broadcast-system

This is a multi-server system for broadcasting messages between clients inplemented in Java.
Communications are via TCP sockets and the messages are sent as JSON objects. 
Features include:
* Clients can broadcast messages to all other clients connnected at the time.
* Allow clients to register a username and secret  pair to act as authentication, or login as anonymous
* Load balancing clients among the servers using redirection mechanism
* Servers will try to connect to other servers in the event of network partioning, e.g. server crashes, to maintain architecture integrity and availability for the clients. I wrote more about this backup mechanism in my blog [here](https://zhitaop.github.io/project/project2/).

As this is project is originally a university project for one of my subjects, I wrote this in a limited time only aiming for correct functionality. The code quality and structure can probably be improved quite a bit, like applying more software design pattern and not making one class 700 lines long. 
