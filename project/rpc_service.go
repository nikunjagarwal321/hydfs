package main

import (
	"fmt"
)

type GrepArgs struct {
	Pattern string
	Options []string
}

type GrepResponse struct {
	Reply []string
}

type RpcService struct {
	ID string
}

func (s *RpcService) ExecuteGrep(args *GrepArgs, response *GrepResponse) error {
	// Use the VM ID that was set when the service was created
	localFile, err := getLogFilename(s.ID)
	if err != nil {
		fmt.Printf("Error getting log filename for VM %s: %v\n", s.ID, err)
		return err
	}

	lines, err := RunGrep(append(args.Options, args.Pattern, localFile)...)
	if err != nil {
		fmt.Printf("Error running grep on %s: %v\n", s.ID, err)
		return err
	}
	fmt.Printf("Found %d lines on current server\n", len(lines))
	*response = GrepResponse{Reply: lines}
	return nil
}
