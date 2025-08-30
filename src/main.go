package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <ClientName>")
		return
	}

	clientName := os.Args[1]

	// Lookup node directly in the map
	currentNode, ok := nodeMap[clientName]
	if !ok {
		fmt.Println("Client name not found in config:", clientName)
		return
	}

	// Start RPC server
	go startRPCServer(currentNode)

	fmt.Printf("[%s] RPC server started on port %s\n", currentNode.ID, currentNode.Port)
	fmt.Println("Press Enter after other clients are running...")
	fmt.Scanln()

	// Broadcast message to peers
	for _, peer := range currentNode.Peers {
		sendMessage(peer, currentNode.ID, "Hello from "+currentNode.ID)
	}

	// Keep server running
	select {}
}
