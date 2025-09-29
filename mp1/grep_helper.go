package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

func RunGrep(args ...string) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no arguments provided to grep")
	}
	grepArgs := []string{}
	for _, arg := range args {
		if arg != "-savelog" {
			grepArgs = append(grepArgs, arg)
		}
	}
	fmt.Println("Running grep with args:", grepArgs) // Log the command
	cmd := exec.Command("grep", grepArgs...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 1 {
			return []string{}, nil
		}
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	return lines, nil
}
