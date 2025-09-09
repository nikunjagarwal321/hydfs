package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run . <NodeName>")
		return
	}
	nodeName := os.Args[1]
	node, ok := nodeMap[nodeName]
	if !ok {
		fmt.Println("Node name not found in config:", nodeName)
		return
	}

	serviceObj := &RpcService{ID: node.ID}
	rpc.Register(serviceObj)

	ln, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		fmt.Println(node.ID, "failed to listen:", err)
		return
	}
	defer ln.Close()

	fmt.Println(node.ID, "listening on port", node.Port)
	go rpc.Accept(ln)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter grep pattern and flags (e.g., 'pattern -i'): ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" || input == "quit" {
			fmt.Println("Exiting...")
			break
		}
		args := strings.Fields(input)
		if len(args) == 0 {
			fmt.Println("Please enter a pattern.")
			continue
		}
		pattern := args[0]
		flags := args[1:]

		localLines, err := RunGrep(append(flags, pattern, "sample.log")...)
		if err != nil {
			fmt.Println("Local grep error:", err)
		} else {
			fmt.Printf("%s:%d:%s:\n", nodeName, len(localLines), "sample.log")
			for _, line := range localLines {
				fmt.Printf("%s:%d:%s: %s\n", nodeName, len(localLines), "sample.log", line)
			}
		}

		for _, peer := range node.Peers {
			fmt.Printf("[Results from %s]\n", peer)
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				fmt.Println("Failed to connect to", peer)
				continue
			}
			defer client.Close()
			grepArgs := &GrepArgs{Pattern: pattern, File: "sample.log", Options: flags}
			var reply GrepResponse
			err = client.Call("RpcService.ExecuteGrep", grepArgs, &reply)
			if err != nil {
				fmt.Println("RPC error from", peer, ":", err)
				continue
			}
			fmt.Printf("%s:%d:%s:\n", peer, len(reply.Reply), "sample.log")
			for _, line := range reply.Reply {
				fmt.Printf("%s:%d:%s: %s\n", peer, len(reply.Reply), "sample.log", line)
			}
		}
	}
}
