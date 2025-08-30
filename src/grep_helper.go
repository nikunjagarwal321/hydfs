package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// RunGrep executes the system grep command with given arguments
// args: any grep options, patterns, and files
// Returns: slice of matching lines
func RunGrep(args ...string) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no arguments provided to grep")
	}

	// Run grep command
	cmd := exec.Command("grep", args...)

	// Capture output
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out // include errors in output

	err := cmd.Run()
	if err != nil {
		// grep returns exit code 1 if no matches are found — not necessarily an error
		if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 1 {
			// no matches, return empty slice
			return []string{}, nil
		}
		return nil, err
	}

	// Split output into lines
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	return lines, nil
}

func main() {
	// Example: case-insensitive search for "hello" in sample.log
	lines, err := RunGrep("-i", "line", "sample.log")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for _, line := range lines {
		fmt.Println(line)
	}
}
