# Distributed System
Group Number : 37
Teammates: nikunja2, akashe2


## Description 
This Golang program implements a distributed membership system supporting two protocols, Gossip and PingAck, each with Suspect and NoSuspect modes. All configurations share a common membership list, and multiple background goroutines handle heartbeats, suspicion and failure tracking, message processing, command-line input, and periodic status updates. Nodes are evaluated for SUSPECT or DEAD status every T time based on configurable thresholds. The system uses a round-robin node selection with configurable fanout and message intervals, enabling timely failure detection, while maintaining moderate bandwidth usage. Both protocols share the suspicion logic, but differ in merging: Gossip propagates heartbeat counters and merges without waiting for acknowledgments, whereas PingAck piggybacks membership info on pings and ACKs and waits for responses. Merge rules prioritize failures, then higher incarnation numbers, followed by suspicion status, and finally heartbeat counters for Gossip, ensuring accurate and consistent membership state across nodes.

## How to Run

### 1. Prerequisites

- Go 1.25+ installed.

### 2. Start Servers
Run servers in each of the servers

In each terminal, run:
```
cd mp2
go run . <VM_ID>
```
Example:

```sh
go run . vm1
go run . vm2
go run . vm3
go run . vm4
go run . vm5
go run . vm6
go run . vm7
go run . vm8
go run . vm9
go run . vm10
```



### 3. Commands that can be run run

- `list_mem`: list the membership list

- `list_self`: list self’s id

- `leave`: voluntarily leave the group (different from a failure)

- `display_suspects` List suspected nodes.

- `switch {gossip, ping} {suspect, nosuspect}` This takes two parameters—it switches the current mechanism to gossip/ping (whichever is first parameter), and without or without suspicion (second parameter)

- `display_protocol` This should output a pair that is <{gossip, ping}, {suspect, nosuspect}>