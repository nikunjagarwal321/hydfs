package main

import (
	"bufio"
	"fmt"
	"os"
)

// GenerateLogFile creates a log file for a specific VM with the given number of lines
func GenerateLogFile(vmID string, lines int) error {
	logFile := fmt.Sprintf("log/%s.log", vmID)

	// Create log file
	f, err := os.Create(logFile)
	if err != nil {
		return fmt.Errorf("failed to create log file %s: %v", logFile, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	frequent := "GET: Get request"
	somewhat := "PUT: Put request"
	rare := "DELETE: Delete request"
	total := lines
	freqCount := int(0.7 * float32(total)) // 70%
	someCount := int(0.2 * float32(total)) // 20%

	// Frequent requests loop
	for i := 1; i <= freqCount; i++ {
		fmt.Fprintf(w, "[Line %d] %s\n", i, frequent)
	}
	// Somewhat frequent requests loop
	for i := 1; i <= someCount; i++ {
		fmt.Fprintf(w, "[Line %d] %s\n", freqCount+i, somewhat)
	}
	// Rare requests loop
	for i := 1; i <= (total - freqCount - someCount); i++ {
		fmt.Fprintf(w, "[Line %d] %s\n", freqCount+someCount+i, rare)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush log file: %v", err)
	}
	fmt.Printf("Generated %d log lines in %s\n", lines, logFile)
	return nil
}
