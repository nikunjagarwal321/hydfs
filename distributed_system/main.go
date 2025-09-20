package main

import (
	"fmt"
	"os"
)

func main() {
	nodeName := os.Args[1]
	fmt.Println("Current node name:", nodeName)
	nodeAddr := NodeMap[nodeName]
	fmt.Println("Current node address:", nodeAddr)
	var isIntroducer = false
	if nodeAddr == Config.IntroducerAddr {
		isIntroducer = true
	}

	s := NewServer(nodeAddr, Config.IntroducerAddr, isIntroducer)
	if err := s.Start(); err != nil {
		fmt.Println("Server error:", err)
	}
}
