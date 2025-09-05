package service

import (
	"fmt"
	"project/utils"
)

// MessageService provides RPC methods
type RpcService struct {
	ID string
}

// BroadcastArgs contains the message and sender ID
type GrepArgs struct {
	Pattern string
	File    string
	Options []string
}

type GrepResponse struct {
	Reply []string
}

// Broadcast prints the message
func (s *RpcService) ExecuteGrep(args *GrepArgs, response *GrepResponse) error {
	//fmt.Printf("[%s] Received from %s: %s\n", s.ID, args.Sender, args.Message)
	lines, err := utils.RunGrep(args.Options, args.Pattern, "sample.log")
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}

	for _, line := range lines {
		fmt.Println(line)
	}

	*response = GrepResponse{
		Reply: lines,
	}

	return nil
}
