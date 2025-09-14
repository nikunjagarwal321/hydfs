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
	frequent := "GET: [17/Aug/2022:18:23:49 -0500] /list HTTP/1.0 301 5090 http://www.sawyer.com/home.htm Mozilla/5.0 (Windows 98; Win 9x 4.90) AppleWebKit/5342 (KHTML, like Gecko) Chrome/14.0.858.0 Safari/5342"
	somewhat := "PUT: [17/Aug/2022:18:23:49 -0500] /list HTTP/1.0 301 5090 http://www.sawyer.com/home.htm Mozilla/5.0 (Windows 98; Win 9x 4.90) AppleWebKit/5342 (KHTML, like Gecko) Chrome/14.0.858.0 Safari/5342"
	rare := "DELETE: [17/Aug/2022:18:23:49 -0500] /list HTTP/1.0 301 5090 http://www.sawyer.com/home.htm Mozilla/5.0 (Windows 98; Win 9x 4.90) AppleWebKit/5342 (KHTML, like Gecko) Chrome/14.0.858.0 Safari/5342"
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
