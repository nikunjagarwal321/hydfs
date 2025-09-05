package main

import (
	"fmt"
	"net/rpc"
	"os"
	"project/config"
	"project/service"
	"strings"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Println("Usage: go run client.go <clientname> <search_pattern> <flags>")
		return
	}

	clientName := os.Args[1]
	searchPattern := os.Args[2]

	// Split flags string into individual flags
	flags := strings.Fields(os.Args[3])

	fmt.Println("Client Name:", clientName)
	fmt.Println("Search Pattern:", searchPattern)
	fmt.Println("Flags:", flags)

	// Lookup node directly in the map
	node, ok := config.NodeMap[clientName]
	if !ok {
		fmt.Println("Client name not found in config:", clientName)
		return
	}

	for _, peer := range node.Peers {
		fmt.Println("Calling peer: ", peer)
		sendMessage(peer, searchPattern, flags)
	}

}

func sendMessage(peerAddr, msg string, flags []string) {
	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		fmt.Println("Failed to connect to", peerAddr)
		return
	}
	defer client.Close()

	args := &service.GrepArgs{Pattern: msg, File: "sample.log", Options: flags}
	var reply service.GrepResponse
	client.Call("RpcService.ExecuteGrep", args, &reply)

	for _, line := range reply.Reply {
		fmt.Println(line)
	}

}
