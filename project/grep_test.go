package main

import (
	"fmt"
	"testing"
	"time"
)

func TestInFrequentPatterns(t *testing.T) {
	fmt.Print("\n======TestInFrequentPatterns=====\n")
	for i := 0; i < 5; i++ {
		pattern := "delete"
		options := []string{"-i"}

		// Use vm1 for testing
		testVMID := "vm1"
		hostname, _ := getCurrentNodeInfo(testVMID)
		if hostname == "" {
			t.Skipf("Skipping test - VM ID %s not found in vmAddressMap", testVMID)
			return
		}
		timestamp := time.Now().Format("20060102_150405")

		// Measure local grep latency
		localStart := time.Now()
		executeLocalGrep(pattern, options, testVMID, timestamp)
		localLatency := time.Since(localStart)

		// Measure remote grep latency
		remoteStart := time.Now()
		executeRemoteGrep(pattern, options, testVMID, timestamp)
		remoteLatency := time.Since(remoteStart)

		totalLatency := localLatency + remoteLatency
		fmt.Printf("InFrequent Pattern (error -i): Local=%v, Remote=%v, Total=%v\n",
			localLatency, remoteLatency, totalLatency)
	}
}

func TestFrequentPatterns(t *testing.T) {
	fmt.Print("\n======TestFrequentPatterns=====\n")
	for i := 0; i < 5; i++ {
		pattern := "get"
		options := []string{"-i"}

		// Use vm1 for testing
		testVMID := "vm1"
		hostname, _ := getCurrentNodeInfo(testVMID)
		if hostname == "" {
			t.Skipf("Skipping test - VM ID %s not found in vmAddressMap", testVMID)
			return
		}
		timestamp := time.Now().Format("20060102_150405")

		// Measure local grep latency
		localStart := time.Now()
		executeLocalGrep(pattern, options, testVMID, timestamp)
		localLatency := time.Since(localStart)

		// Measure remote grep latency
		remoteStart := time.Now()
		executeRemoteGrep(pattern, options, testVMID, timestamp)
		remoteLatency := time.Since(remoteStart)

		totalLatency := localLatency + remoteLatency
		fmt.Printf("Frequent Pattern (info -i): Local=%v, Remote=%v, Total=%v\n",
			localLatency, remoteLatency, totalLatency)
	}
}

func TestSomewhatFrequentPatterns(t *testing.T) {
	fmt.Print("\n======TestSomewhatFrequentPatterns=====\n")
	for i := 0; i < 5; i++ {
		pattern := "put"
		options := []string{"-i"}

		// Use vm1 for testing
		testVMID := "vm1"
		hostname, _ := getCurrentNodeInfo(testVMID)
		if hostname == "" {
			t.Skipf("Skipping test - VM ID %s not found in vmAddressMap", testVMID)
			return
		}
		timestamp := time.Now().Format("20060102_150405")

		// Measure local grep latency
		localStart := time.Now()

		executeLocalGrep(pattern, options, testVMID, timestamp)
		localLatency := time.Since(localStart)

		// Measure remote grep latency
		remoteStart := time.Now()

		executeRemoteGrep(pattern, options, testVMID, timestamp)
		remoteLatency := time.Since(remoteStart)

		totalLatency := localLatency + remoteLatency
		fmt.Printf("Somewhat Frequent Pattern (debug -i): Local=%v, Remote=%v, Total=%v\n",
			localLatency, remoteLatency, totalLatency)
	}
}

func TestGrepOptions(t *testing.T) {
	fmt.Print("\n======TestGrepOptions=====\n")
	for i := 0; i < 5; i++ {
		pattern := "get"
		options := []string{"-c"}

		// Use vm1 for testing
		testVMID := "vm1"
		hostname, _ := getCurrentNodeInfo(testVMID)
		if hostname == "" {
			t.Skipf("Skipping test - VM ID %s not found in vmAddressMap", testVMID)
			return
		}
		timestamp := time.Now().Format("20060102_150405")
		// Measure local grep latency
		localStart := time.Now()
		executeLocalGrep(pattern, options, testVMID, timestamp)
		localLatency := time.Since(localStart)

		// Measure remote grep latency
		remoteStart := time.Now()

		executeRemoteGrep(pattern, options, testVMID, timestamp)
		remoteLatency := time.Since(remoteStart)

		totalLatency := localLatency + remoteLatency
		fmt.Printf("Grep Options (INFO -c): Local=%v, Remote=%v, Total=%v\n",
			localLatency, remoteLatency, totalLatency)
	}
}

func TestLogGeneration(t *testing.T) {
	fmt.Print("\n======TestLogGeneration=====\n")

	// Test generating logs for a few VMs
	testVMs := []string{"vm1", "vm2", "vm3", "vm4"}
	testLines := 300000

	for _, vmID := range testVMs {
		fmt.Printf("Generating log for %s...\n", vmID)
		err := GenerateLogFile(vmID, testLines)
		if err != nil {
			t.Errorf("Failed to generate log for %s: %v", vmID, err)
			continue
		}
	}
}
