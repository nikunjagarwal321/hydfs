package main

import (
	"fmt"
	"net"
	"net/rpc"
)

func startRPCServer(node NodeConfig) {
	service := &MessageService{ID: node.ID}
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
