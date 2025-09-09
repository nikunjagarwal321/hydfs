package main

import (
	"fmt"
)

type GrepArgs struct {
	Pattern string
	File    string
	Options []string
}

type GrepResponse struct {
	Reply []string
}

type RpcService struct {
	ID string
}

func (s *RpcService) ExecuteGrep(args *GrepArgs, response *GrepResponse) error {
	lines, err := RunGrep(append(args.Options, args.Pattern, args.File)...)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	*response = GrepResponse{Reply: lines}
	return nil
}
