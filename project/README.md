# Distributed Grep System

## Project Structure

- `main.go`: The entry point for the distributed grep server. Run this file to start a node.
- `config.go`: Contains the configuration for nodes (name, port, peers).
- `rpc_service.go`: Defines the RPC service and the arguments/response for the grep operation.
- `grep_helper.go`: Helper to run the system `grep` command and return results.
- `sample.log`: Sample log file used for grep operations.
- `go.mod`: Go module definition.

## How the System Works

- Each node is defined in `config.go` with a name, port, and list of peer addresses.
- The server (`main.go`) starts an RPC server on its configured port and waits for grep requests.
- While running, you can enter grep commands interactively in the terminal. The command is executed locally and also sent to all peers using RPC.
- The RPC service (`rpc_service.go`) receives grep requests, runs the `grep` command on `sample.log` using the provided pattern and flags, and returns the results.
- The actual `grep` command being run is printed as a log for debugging.

## How to Run

### 1. Prerequisites

- Go 1.25+ installed.
- The `grep` command available on your system (standard on Unix/Linux/macOS).

### 2. Start Servers

Open three terminal windows (one for each node: NodeA, NodeB, NodeC).

In each terminal, run:

```sh
cd /Users/nikunjagarwal/Desktop/uiuc_mcs/CS425/distributed_query_log/project
go run . <NodeName>
```

Replace `<NodeName>` with one of: `NodeA`, `NodeB`, `NodeC`.

Example:

```sh
go run . NodeA
```

### 3. Run Grep Commands

While the server is running, type a pattern and optional flags at the prompt. For example:

```
line -i
```

- This will search for the pattern `line` (case-insensitive) in `sample.log` on all nodes.
- The results from the local node and all peers will be printed.

Type `exit` or `quit` to stop the server.

## Notes

- The log file being searched is always `sample.log` in the project directory.
- You can edit or replace `sample.log` with your own log data.
- The system is designed for local testing; all nodes are configured to use `localhost` and different ports.
- The actual `grep` command being run is printed as a log for debugging.
