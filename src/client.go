package main

import (
	"fmt"
	"net/rpc"
)

func sendMessage(peerAddr, senderID, msg string) {
	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		fmt.Println("Failed to connect to", peerAddr)
		return
	}
	defer client.Close()

	args := &BroadcastArgs{Sender: senderID, Message: msg}
	var reply string
	client.Call("MessageService.Broadcast", args, &reply)
}
