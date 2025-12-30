# Distributed System
Group Number : 37
Teammates: nikunja2, akashe2


## Description 
This project is a distributed file storage system built in Go, designed for high availability, fault tolerance, and efficient data transfer. It uses the Gossip Protocol for membership management, RPC for control operations, and gRPC with Protobuf for high-performance data transfer. The system maintains a replication factor of three, tolerating up to two node failures. In case of a failure, data remains available through surviving replicas, and the merge and re-replication mechanisms automatically restore lost copies.

The system follows an eventual consistency model, where a write is acknowledged after any one replica responds, and background protocols ensure data eventually propagates to all replicas. Appends are stored as separate files with their order preserved in metadata, and each carries a timestamp and AppendID to maintain ordering. A periodic merge process verifies checksums and synchronizes files and metadata, resolving conflicts using the primary’s metadata as the source of truth.

When a node or its neighbors fail, re-replication is triggered, ensuring primary nodes restore missing files. New nodes joining the system fetch the required data, while a garbage collector removes redundant files outside their replication range. Overall, this system provides a lightweight, reliable, and self-healing storage solution with automated recovery and consistency maintenance.

## How to Run

### 1. Prerequisites

- Go 1.25+ installed.

### 2. Start Servers
Run servers in each of the servers. Starts rpc and grpc servers in ports 8080 and 9080 respectively

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

### 3. CLI Commands

- `list_self`: print this node’s membership identifier.
- `list_mem_ids`: show the current membership list with hashes and status.
- `display_protocol`: report the active `{protocol, suspicion}` pair and drop rate.
- `switch {gossip|ping} {suspect|nosuspect}`: change the membership protocol and suspicion mode and broadcast to peers.
- `display_suspects`: list nodes currently marked as suspects.
- `drop <percentage>`: inject simulated message loss (e.g., `drop 0.2` for 20%).
- `leave`: issue a voluntary leave announcement.
- `create <local> <hyDFS>`: upload a local file into HyDFS.
- `get <hyDFS> <local>`: download a HyDFS file into the local directory.
- `append <local> <hyDFS>`: append data from a local file to a HyDFS file.
- `merge <hyDFS>`: trigger metadata/file reconciliation for the named file.
- `multiappend <hyDFS> <vm...> <local...>`: orchestrate parallel appends from multiple VMs.
- `printmeta <hyDFS>`: display stored metadata (including append order) for a file.
- `ls <hyDFS>`: list replicas that currently store the file.
- `liststore`: enumerate all files stored on this VM along with their IDs.
- `getfromreplica <vm> <hyDFS> <local>`: fetch a replica directly from a specific VM.