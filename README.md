# Key-Value-Store
Implement a fault-tolerant key-value store using a replication strategy that provides causal consistency. The causal consistency model captures the causal relationship between operations and ensures that all clients see the operations in causal order regardless of which replica they contact to do the operations.

Files:

Dockerfile
kvsProj.py
mechanism-description.txt
contributions.yml
Defintion:

Causal Consistency: Writes causally related must happen in the same (causal) order for all process. We want to satisfy the conditions below:
current operation depends on the clients past writes.
Three principles of Happens Before Relation
Causal Dependency: 2 causally depends on version V1 if PUT(Key1, Value1) --> PUT(Key2, Value2)
Visibilty: Return the latest value written by PUT/DELETE operation.
Whats an event?

PUT request to write a new key
PUT request to update the value of a key
DELETE request to remove the key
GET request to get the value of a key
Causal Metadata:

To track causal dependencies in request and response messages.
Use Vector Clock to get Causal metadata.
Attach this clock value to messages.
Conflict Resolution for Eventual Consistency:

Gossip protocol- to find the conflicts and use last-write-wins technique. -> Do this with a timestamp value added by the client to the PUT operation.
During GET request for a specific key, FORWARD the request to other replicas and COMPARE the value from all replicas including itself. Select one value as final value and ask other replicas to set that value as well.
Converge after 10 seconds.
