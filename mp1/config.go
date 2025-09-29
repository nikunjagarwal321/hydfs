package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// VM address to VM ID mapping (includes hostname:port)
var vmAddressMap = map[string]string{
	"vm1":  "localhost:8081",
	"vm2":  "localhost:8082",
	"vm3":  "localhost:8083",
	"vm4":  "localhost:8084",
	"vm5":  "localhost:8085",
	"vm6":  "localhost:8086",
	"vm7":  "localhost:8087",
	"vm8":  "localhost:8088",
	"vm9":  "localhost:8089",
	"vm10": "localhost:8080",
}

//	var vmAddressMap = map[string]string{
//		"vm1":  "fa25-cs425-3701.cs.illinois.edu:8080",
//		"vm2":  "fa25-cs425-3702.cs.illinois.edu:8080",
//		"vm3":  "fa25-cs425-3703.cs.illinois.edu:8080",
//		"vm4":  "fa25-cs425-3704.cs.illinois.edu:8080",
//		"vm5":  "fa25-cs425-3705.cs.illinois.edu:8080",
//		"vm6":  "fa25-cs425-3706.cs.illinois.edu:8080",
//		"vm7":  "fa25-cs425-3707.cs.illinois.edu:8080",
//		"vm8":  "fa25-cs425-3708.cs.illinois.edu:8080",
//		"vm9":  "fa25-cs425-3709.cs.illinois.edu:8080",
//		"vm10": "fa25-cs425-3710.cs.illinois.edu:8080",
//	}
//
// VMInfo represents a VM with its ID and address
type VMInfo struct {
	ID      string
	Address string
}

// Get all VM info (ID and address) except the current one
func getAllOtherVMs(currentVMID string) []VMInfo {
	var vms []VMInfo
	for vmID, address := range vmAddressMap {
		if vmID != currentVMID {
			vms = append(vms, VMInfo{ID: vmID, Address: address})
		}
	}
	return vms
}

// Get hostname and port from VM ID
func getHostnameAndPortFromVMID(vmID string) (string, string) {
	address := vmAddressMap[vmID]
	if address == "" {
		fmt.Println("no address found in map")
		return "", ""
	}
	parts := strings.Split(address, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	// Some other error (e.g., permission issue)
	return false
}

// Get log filename from vmId
func getLogFilename(vmID string) (string, error) {
	fileName := "log/" + vmID + ".log"
	if fileExists(fileName) {
		return fileName, nil
	}
	return "", fmt.Errorf("file : %s does not exist", fileName)
}

// Ensure output directory exists
func ensureOutputDir() error {
	return os.MkdirAll("output", 0755)
}

func shouldSaveLog(flags []string) bool {
	for _, f := range flags {
		if f == "-savelog" {
			return true
		}
	}
	return false
}

// Save grep output to file
func saveGrepOutput(timestamp, vmID string, reply []string) error {
	if err := ensureOutputDir(); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	filename := filepath.Join("output", fmt.Sprintf("%s.%s.log", timestamp, vmID))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %v", filename, err)
	}
	defer file.Close()

	// Write all matching lines directly
	for _, line := range reply {
		fmt.Fprintln(file, line)
	}

	return nil
}
