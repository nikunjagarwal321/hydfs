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
			ConsolePrintf("Invalid append command format. Expected: append <localfilename> <HyDFSfilename>\n")
			return
		}
		s.handleAppend(parts[1], parts[2])
	case "merge":
		if len(parts) != 2 {
			ConsolePrintf("Invalid merge command format. Expected: merge <HyDFSfilename>\n")
			return
		}
		s.handleMergeCommand(parts[1])
	case "multiappend":
		if len(parts) < 4 {
			ConsolePrintf("Invalid multiappend command format. Expected: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>\n")
			return
		}
		hyDFSfilename, vmNames, localFiles := s.parseMultiAppend(parts[1:])
		s.handleMultiAppend(hyDFSfilename, vmNames, localFiles)
	case "printmeta": //for testing purposes
		if len(parts) != 2 {
			ConsolePrintf("Invalid printmeta command format. Expected: printmeta <HyDFSfilename>\n")
			return
		}
		s.Metadata.PrintFileMetadata(parts[1])
	case "ls":
		if len(parts) != 2 {
			ConsolePrintf("Invalid ls command format. Expected: ls <HyDFSfilename>\n")
			return
		}
		s.handleLS(parts[1])
	case "liststore":
		s.handleListStore()
	case "list_mem_ids":
		s.Members.Print(true)
	case "getfromreplica":
		if len(parts) != 4 {
			ConsolePrintf("Invalid getfromreplica command format. Expected: getfromreplica <VMID> <HyDFSfilename> <localfilename>\n")
			return
		}
		s.handleGetFromReplica(parts[1], parts[2], parts[3])
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

// parseMultiAppend parses multiappend command: multiappend HyDFSfilename VM1 ... VMN localfile1 ... localfileN
// Format: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>
// VM format: vm1, vm2, vm3, etc.
func (s *Server) parseMultiAppend(args []string) (string, []string, []string) {
	if len(args) < 3 {
		ConsolePrintf("Invalid multiappend: need at least HyDFSfilename, one VM, and one local file\n")
		return "", nil, nil
	}

	hyDFSfilename := args[0]

	// Validate that first argument is not a VM name
	if strings.HasPrefix(strings.ToLower(hyDFSfilename), "vm") {
		ConsolePrintf("Invalid multiappend: first argument should be HyDFSfilename, not VM name. Format: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>\n")
		return "", nil, nil
	}

	// Parse arguments: VMs are vm1, vm2, etc., local files follow after all VMs
	var vmNames []string
	var localFiles []string

	// Find all consecutive VM names starting from index 1
	// VM names should start with "vm" (case-insensitive)
	splitIndex := -1
	for i := 1; i < len(args); i++ {
		if strings.HasPrefix(strings.ToLower(args[i]), "vm") {
			vmNames = append(vmNames, args[i])
		} else {
			// First non-VM argument marks the start of local files
			splitIndex = i
			break
		}
	}

	if splitIndex == -1 {
		ConsolePrintf("Invalid multiappend: could not find local files. Format: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>\n")
		return "", nil, nil
	}

	localFiles = args[splitIndex:]

	// Validate: number of VMs should match number of local files
	if len(vmNames) != len(localFiles) {
		ConsolePrintf("Invalid multiappend: number of VMs (%d) must match number of local files (%d)\n",
			len(vmNames), len(localFiles))
		return "", nil, nil
	}

	if len(vmNames) == 0 {
		ConsolePrintf("Invalid multiappend: at least one VM is required\n")
		return "", nil, nil
	}

	ConsolePrintf("Parsed multiappend: HyDFSfilename=%s, VMs=%v, LocalFiles=%v\n",
		hyDFSfilename, vmNames, localFiles)

	return hyDFSfilename, vmNames, localFiles
}
