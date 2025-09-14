package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

func getLocalHostname() string {
	out, err := exec.Command("hostname").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func getLogFileFromHostname() string {
	hostname := getLocalHostname()
	//print(hostname)
	parts := strings.Split(hostname, "-")
	if len(parts) <= 2 {
		return "sample.log"
	}
	num := parts[len(parts)-1] // e.g., "3701.cs.illinois.edu"
	num = strings.Split(num, ".")[0]
	if len(num) >= 2 {
		lastTwo := num[len(num)-2:]
		return fmt.Sprintf("machine.%s.log", lastTwo)
	}
	return "sample.log"
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run . <NodeName>")
		return
	}
	nodeName := os.Args[1]
	node, ok := nodeMap[nodeName]
	if !ok {
		fmt.Println("Node name not found in config:", nodeName)
		return
	}

	serviceObj := &RpcService{ID: node.ID}
	rpc.Register(serviceObj)

	ln, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		fmt.Println(node.ID, "failed to listen:", err)
		return
	}
	defer ln.Close()

	fmt.Println(node.ID, "listening on port", node.Port)
	go rpc.Accept(ln)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter grep pattern and flags (e.g., 'pattern -i'): ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" || input == "quit" {
			fmt.Println("Exiting...")
			break
		}
		args := strings.Fields(input)
		if len(args) == 0 {
			fmt.Println("Please enter a pattern.")
			continue
		}
		pattern := args[0]
		flags := args[1:]

		// Local grep
		localFile := getLogFileFromHostname()
		localLines, err := RunGrep(append(flags, pattern, localFile)...)
		if err != nil {
			fmt.Println("Local grep error:", err)
		} else {
			fmt.Printf("localhost:%s:%s:Number of lines:%d\n", node.Port, localFile, len(localLines))
		}

		// Parallel RPC calls to peers
		var wg sync.WaitGroup
		for _, peer := range node.Peers {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()

				client, err := rpc.Dial("tcp", peer)
				if err != nil {
					fmt.Println("Failed to connect to", peer)
					return
				}
				defer client.Close()

				grepArgs := &GrepArgs{Pattern: pattern, File: localFile, Options: flags}
				var reply GrepResponse

				done := make(chan error, 1)
				go func() {
					done <- client.Call("RpcService.ExecuteGrep", grepArgs, &reply)
				}()

				select {
				case err := <-done:
					if err != nil {
						fmt.Println("RPC error from", peer, ":", err)
						return
					}
					fmt.Printf("%s:%s:Number of lines:%d\n", peer, localFile, len(reply.Reply))
				case <-time.After(5 * time.Second):
					fmt.Println("Timeout from", peer)
				}
			}(peer)
		}
		wg.Wait()
	}
}
