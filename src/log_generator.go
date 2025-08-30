package main

import (
	"fmt"
	"os"
	"time"
)

// GenerateLogs creates a log file with n lines
// filename: name of the log file to create
// n: number of lines
func GenerateLogs(filename string, n int) error {
	// Create or truncate the file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Generate n lines
	for i := 1; i <= n; i++ {
		// Example log format: timestamp + log message
		line := fmt.Sprintf("%s - INFO - Log line number %d\n", time.Now().Format(time.RFC3339), i)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	filename := "sample.log"
	n := 300000 // number of lines

	err := GenerateLogs(filename, n)
	if err != nil {
		fmt.Println("Error generating logs:", err)
		return
	}

	fmt.Printf("Generated %d lines in %s\n", n, filename)
}
