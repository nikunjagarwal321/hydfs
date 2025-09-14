package main

import "fmt"

// MessageService provides RPC methods
type MessageService struct {
	ID string
}

// BroadcastArgs contains the message and sender ID
type BroadcastArgs struct {
	Sender  string
	Message string
}

// Broadcast prints the message
func (s *MessageService) Broadcast(args *BroadcastArgs, reply *string) error {
	fmt.Printf("[%s] Received from %s: %s\n", s.ID, args.Sender, args.Message)

// Call grep implementation here
	


	*reply = "OK"
	return nil
}