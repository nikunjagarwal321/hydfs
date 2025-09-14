package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Gets hostname, port based on vmId
func getCurrentNodeInfo(vmID string) (string, string) {
	hostname, port := getHostnameAndPortFromVMID(vmID)
	return hostname, port
}

// Registers the rpc service object
func setupRPCService(vmID string) {
	serviceObj := &RpcService{ID: vmID}
	rpc.Register(serviceObj)
}

// Sets up the listener for the servers.
func setupNetworkListener(port string, vmID string) (net.Listener, error) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println(vmID, "failed to listen:", err)
		return nil, err
	}
	fmt.Println(vmID, "listening on port", port)
	// This is the go routine. It opens a new thread and waits for clients to conenct
	go rpc.Accept(ln)
	return ln, nil
}

// THis function parses user input to get patterns and flags
// First option is pattern/regex
// Second option is flag
func parseUserInput(reader *bufio.Reader) (string, []string) {
	fmt.Print("Enter grep pattern: ")
	pattern, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input:", err)
		return "", nil
	}

	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		fmt.Println("Please enter a pattern. No pattern entered")
		return "", nil
	}

	fmt.Print("Enter grep flags (space-separated, or leave blank): ")
	flagsInput, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading flags:", err)
		return pattern, nil
	}
	flagsInput = strings.TrimSpace(flagsInput)
	flags := strings.Fields(flagsInput) // split by spaces

	return pattern, flags
}

func savePeerOutput(peerVMID, timestamp, peer string, reply []string) {
	if peerVMID != "unknown" {
		if saveErr := saveGrepOutput(timestamp, peerVMID, reply); saveErr != nil {
			fmt.Printf("Warning: Failed to save output from %s: %v\n", peer, saveErr)
		}
	}
}

// Executes grep locally. It fetches log file name based on vmId. Runs grep on it and saves the result in a file.
// timestamp is used for naming the output file.
// Returns the number of matching lines found locally.
func executeLocalGrep(pattern string, flags []string, vmID string, timestamp string) int {
	localFile, err := getLogFilename(vmID)
	if err != nil {
		fmt.Println("Error getting log filename:", err)
		return 0
	}

	localLines, err := RunGrep(append(flags, pattern, localFile)...)
	if err != nil {
		fmt.Println("Local grep error:", err)
		return 0
	}

	// Display results
	fmt.Printf("Number of lines from local:%d\n", len(localLines))

	// Save output to file
	if shouldSaveLog(flags) {
		if saveErr := saveGrepOutput(timestamp, vmID, localLines); saveErr != nil {
			fmt.Printf("Warning: Failed to save local output: %v\n", saveErr)
		}
	}

	return len(localLines)
}

// Runs the remote grep on all other servers
// Returns the total number of matching lines found across all remote servers
func executeRemoteGrep(pattern string, flags []string, otherVMs []VMInfo, timestamp string) int {
	var wg sync.WaitGroup
	var totalRemoteLines int64 //for atomic operations

	for _, vm := range otherVMs {
		wg.Add(1)
		// Run connect and grep on each server parallely using go routines.
		go func(vmInfo VMInfo) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", vmInfo.Address)
			if err != nil {
				fmt.Println("Failed to connect to", vmInfo.Address)
				return
			}
			defer client.Close()

			grepArgs := &GrepArgs{Pattern: pattern, Options: flags}
			var reply GrepResponse
			done := make(chan error, 1)
			go func() {
				done <- client.Call("RpcService.ExecuteGrep", grepArgs, &reply)
			}()

			select {
			case err := <-done:
				if err != nil {
					fmt.Println("RPC error from", vmInfo.Address, ":", err)
					return
				}

				// Display results
				fmt.Printf("Number of lines from %s::%d\n", vmInfo.Address, len(reply.Reply))

				// Save output to file
				if shouldSaveLog(flags) {
					savePeerOutput(vmInfo.ID, timestamp, vmInfo.Address, reply.Reply)
				}

				// Atomically add to total count
				atomic.AddInt64(&totalRemoteLines, int64(len(reply.Reply)))

			case <-time.After(5 * time.Second):
				fmt.Println("Timeout from", vmInfo.Address)
			}
		}(vm)
	}
	wg.Wait()

	return int(totalRemoteLines)
}

// Execute local and remote grep concurrently
func executeParallelGrep(pattern string, flags []string, vmID string, vmList []VMInfo, timestamp string) int {
	localCh := make(chan int)
	remoteCh := make(chan int)

	go func() { localCh <- executeLocalGrep(pattern, flags, vmID, timestamp) }()
	go func() { remoteCh <- executeRemoteGrep(pattern, flags, vmList, timestamp) }()

	return <-localCh + <-remoteCh
}

// Runs the server chat in interactive mode. Users can enter patterns and flags multiple times for grep.
// It will first run local grep and then push grep to remote servers.
func runInteractiveMode(vmID string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		pattern, flags := parseUserInput(reader)
		fmt.Printf("Pattern = %s, flags = %s\n", pattern, flags)
		if pattern == "" {
			fmt.Println("No Pattern found input")
			continue
		}

		timestamp := time.Now().Format("20060102_150405")

		fmt.Printf("Filename timestamp:%s\n", timestamp)
		vmList := getAllOtherVMs(vmID)
		// Execute grep on all servers (local and remote) simultaneously
		totalLineCount := executeParallelGrep(pattern, flags, vmID, vmList, timestamp)

		fmt.Printf("Total lines found across all servers: %d\n", totalLineCount)
	}
}

func main() {
	// Get VM ID from command line arguments
	if len(os.Args) < 2 {
		fmt.Println("Provide vm id in args")
		return
	}

	vmID := os.Args[1]

	// Get node info based on VM ID
	hostname, port := getCurrentNodeInfo(vmID)

	if hostname == "" || port == "" {
		fmt.Printf("No VM Mapping found for %s\n", vmID)
		return
	}
	fmt.Printf("Starting %s on %s:%s\n", vmID, hostname, port)
	setupRPCService(vmID)
	ln, err := setupNetworkListener(port, vmID)
	if err != nil {
		return
	}
	defer ln.Close()
	// Runs the main program in interactive mode.
	// On the same terminal, we can enter grep options. It will act as a client and call other servers to get grep output
	runInteractiveMode(vmID)
}

// Generates log file in other vms
func executeFileGenerationInOtherVMs(vmID string, lines int) {
	var wg sync.WaitGroup
	otherVMs := getAllOtherVMs(vmID)
	for _, vm := range otherVMs {
		wg.Add(1)
		// Run connect and grep on each server parallely using go routines.
		go func(vmInfo VMInfo) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", vmInfo.Address)
			if err != nil {
				fmt.Println("Failed to connect to", vmInfo.Address)
				return
			}
			defer client.Close()

			generateFileArgs := &GenerateFileArgs{Lines: lines}
			var reply GenerateFileResponse
			done := make(chan error, 1)
			go func() {
				done <- client.Call("RpcService.GenerateLogFile", generateFileArgs, &reply)
			}()

			select {
			case err := <-done:
				if err != nil {
					fmt.Println("RPC error from", vmInfo.Address, ":", err)
					return
				}

				// Display results
				fmt.Printf("Message from %s VM : %s\n", vmInfo.Address, reply.Message)

			case <-time.After(5 * time.Second):
				fmt.Println("Timeout from", vmInfo.Address)
			}
		}(vm)
	}
	wg.Wait()
}
