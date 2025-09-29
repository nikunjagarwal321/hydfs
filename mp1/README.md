# Distributed Log Query

## How to Run

### 1. Prerequisites

- Go 1.25+ installed.
- The `grep` command available on your system (standard on Unix/Linux/macOS).

### 2. Start Servers
Run servers in each of the servers

In each terminal, run:
```
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



### 3. Run Grep Commands

While the server is running, type a pattern and optional flags at the prompt. For example:

```
line -i
```