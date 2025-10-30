package main

import (
	"bufio"
	"os"
	"strings"
)

func (s *Server) startCLI(cmdChan chan<- string) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmdChan <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		LogError(true, "Error reading input: %v", err)
	}
}

func (s *Server) handleCommand(cmd string) {
	// Split command into parts at the beginning
	parts := strings.Fields(strings.TrimSpace(cmd))
	if len(parts) == 0 {
		return
	}

	command := parts[0]

	switch command {
	case "list_mem":
		s.Members.Print(true)
	case "list_self":
		ConsolePrintf("Self ID: %s\n", s.ID())
	case "leave":
		s.leaveGroup()
	case "display_suspects":
		s.Members.PrintSuspectedNodes()
	case "display_protocol":
		printCurrentProtocol()
	case "switch":
		if len(parts) != 3 {
			ConsolePrintf("Invalid switch command format. Expected: switch <protocol> <suspicion>\n")
			return
		}
		s.handleSwitch(parts[1], parts[2])
	case "drop":
		if len(parts) != 2 {
			ConsolePrintf("Invalid drop command format. Expected: drop <percentage>\n")
			return
		}
		SetMessageDropRate(parts[1])
	// Usage: create text file in mp2 directory and call command using "create <local filename> <HyDFS filename>"
	case "create":
		if len(parts) != 3 {
			ConsolePrintf("Invalid create command format. Expected: create <localfilename> <HyDFSfilename>\n")
			return
		}
		s.handleCreate(parts[1], parts[2])
	case "get":
		if len(parts) != 3 {
			ConsolePrintf("Invalid get command format. Expected: get <HyDFSfilename> <localfilename>\n")
			return
		}
		s.handleGet(parts[1], parts[2])
	case "append":
		if len(parts) != 3 {
			ConsolePrintf("Invalid appen command format. Expected: append <localfilename> <HyDFSfilename>\n")
			return
		}
		s.handleAppend(parts[1], parts[2])

	//TODO: Implement get, merge and append for HyDFS
	default:
		ConsolePrintf("Unknown command: %s\n", command)
	}
}

func (s *Server) leaveGroup() {
	LogInfo(true, "Initiating graceful leave from the group...")

	// Get current membership snapshot
	snapshot := s.Members.Snapshot()
	selfMember, exists := snapshot[s.ID()]

	if !exists {
		LogError(true, "Error: Self not found in membership list")
		return
	}

	// Mark self as voluntarily leaving
	selfMember.MarkVoluntaryLeave()
	selfMember.Incarnation = s.IncarnationNumber // Use current incarnation
	s.Members.AddOrUpdate(selfMember)

	LogInfo(true, "MEMBER_LEAVE: Marked self as voluntarily leaving: %s", s.ID())
}

func (s *Server) handleSwitch(protocolStr, suspicionStr string) {
	protocolStr = strings.ToLower(protocolStr)
	suspicionStr = strings.ToLower(suspicionStr)

	protocol, ok := ProtocolMap[protocolStr]
	if !ok {
		ConsolePrintf("Invalid protocol '%s'\n", protocolStr)
		return
	}

	suspicion, ok := SuspicionMap[suspicionStr]
	if !ok {
		ConsolePrintf("Invalid suspicion type '%s'\n", suspicionStr)
		return
	}

	SwitchProtocol(protocol, suspicion)

	// Broadcast protocol switch to all other nodes in the membership list
	snapshot := s.Members.Snapshot()

	for _, member := range snapshot {
		// Skip self
		if member.Address != s.Addr {
			go func(nodeAddr string) {
				resp, err := CallProtocolSwitch(nodeAddr, s.ID(), protocol, suspicion, s)
				if err != nil {
					LogError(true, "Failed to send protocol switch to %s: %v", nodeAddr, err)
				} else {
					LogInfo(true, "Sent protocol switch to %s, success: %v", nodeAddr, resp.Success)
				}
			}(member.Address)
		}
	}

	ConsolePrintln("Protocol switch broadcast completed")
}

func printCurrentProtocol() {
	ConsolePrintf("Current Protocol: %s | Suspicion: %s | Message Drop Rate: %.2f%%\n",
		Config.Protocol, Config.Suspicion, Config.MessageDropRate*100)
}
