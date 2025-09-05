package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"project/config"
	"project/service"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Usage: go run server.go <ClientName>")
		return
	}
	clientName := os.Args[1]
	// Lookup node directly in the map
	node, ok := config.NodeMap[clientName]
	if !ok {
		fmt.Println("Client name not found in config:", clientName)
		return
	}

	service := &service.RpcService{ID: node.ID}
	rpc.Register(service)

	ln, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		fmt.Println(node.ID, "failed to listen:", err)
		return
	}
	defer ln.Close()

	fmt.Println(node.ID, "listening on port", node.Port)
	rpc.Accept(ln)
}
