Intoduce Flow
1. Write a UDP connection which others can join to, and message : NEW_JOINEE
2. Create membership list structure
3. Test that a new joinee sends a NEW_JOINEE message to Introducer

Implement Background Heartbeat Check
1. Every x seconds a process runs and checks for Suspicion
2. If suspicion node is there -> handle

Gossip Protocol implement 
1.  Select b random nodes(create a function) and send own membership list along with suspected nodes
2.  Implement receiving of this list and merging it
3.  Periodic gossip sending every background check interval
4.  Intelligent membership merging with incarnation and heartbeat comparison
5.  Protocol switching capability for future SWIM implementation

SWIM Protocol implement
1. Send Ping to random node
2. Send ACK from that node
3. Send Ping Req to a node
4. Call Suspect from that node
5. Return with Actual ALIVE message with updated Inc number
6. Send back the ACK_ALIVE back

Incarnation number logic
1. 

