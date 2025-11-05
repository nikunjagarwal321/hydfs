package main

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"time"
)

func (s *Server) handleCreate(localFilename, hyDFSfilename string) {
	// Hash the HyDFS filename to determine target nodes
	fileHash := HashToMbits(hyDFSfilename)
	ConsolePrintf("File hash for %s: %s\n", hyDFSfilename, fileHash.String())

	// Find target nodes (primary + replicas)
	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)

	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	// Read local file
	fileData, err := os.ReadFile(localFilename)
	if err != nil {
		ConsolePrintf("Error reading file %s: %v\n", localFilename, err)
		return
	}

	ConsolePrintf("Read file %s (%d bytes)\n", localFilename, len(fileData))

	// Create file metadata
	fileContentHash := fmt.Sprintf("%x", sha256.Sum256(fileData))
	creationTime := fmt.Sprintf("%d", time.Now().Unix())

	fileMetadata := FileMetadata{
		FileName:        hyDFSfilename,
		FileContentHash: fileContentHash,
		FileNameHash:    fileHash,
		CreationTime:    creationTime,
		Appends:         []AppendInfo{},
	}

	ConsolePrintf("Created file metadata: %+v\n", fileMetadata)

	// Send file to target nodes (W=1, R=1: wait for only 1 successful response)
	successChan := make(chan bool, len(targetMembers))
	doneChan := make(chan struct{})

	for _, targetMember := range targetMembers {
		go func(addr string) {
			ConsolePrintf("Sending file to %s...\n", convertToGRPCAddress(addr))
			if err := s.SendFileToNode(addr, fileData, fileMetadata); err != nil {
				ConsolePrintf("Error sending file to %s: %v\n", convertToGRPCAddress(addr), err)
				successChan <- false
			} else {
				successChan <- true
			}
		}(targetMember.Address)
	}

	go func() {
		successCount := 0
		for i := 0; i < len(targetMembers); i++ {
			if <-successChan {
				successCount++
			}
		}
		if successCount == 0 {
			ConsolePrintf("CREATE FAILED: all replicas failed for file %s\n", hyDFSfilename)
		} else {
			ConsolePrintf("File creation completed: %s -> %s (received %d/%d responses)\n", localFilename, hyDFSfilename, successCount, len(targetMembers))
		}
		close(doneChan)
	}()

	<-doneChan
}

func (s *Server) handleGet(hyDFSfilename, localFilename string) {
	// Hash the HyDFS filename to determine target nodes
	fileHash := HashToMbits(hyDFSfilename)

	// Find target nodes (primary + replicas)
	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)

	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	// Use gRPC GetFile similar to handleCreate's SendFileToNode
	downloadFromNode := func(address string) error {
		return s.ReceiveFileFromNode(address, hyDFSfilename, localFilename)
	}

	// Try primary first, then replicas sequentially. Each attempt has ReadTimeout.
	for _, m := range targetMembers {
		addr := m.Address
		done := make(chan error, 1)
		go func() { done <- downloadFromNode(addr) }()

		select {
		case err := <-done:
			if err == nil {
				return
			}
			ConsolePrintf("GET error from %s: %v\n", convertToGRPCAddress(addr), err)
			// try next replica
		case <-time.After(ReadTimeout):
			ConsolePrintf("GET timeout from %s after %s\n", convertToGRPCAddress(addr), ReadTimeout)
			// try next replica
		}
	}

	ConsolePrintf("GET FAILED: no node could serve %s\n", hyDFSfilename)
}

func (s *Server) handleAppend(localFilename, hyDFSfilename string) {
	fileHash := HashToMbits(hyDFSfilename)
	ConsolePrintf("File hash for %s: %s\n", hyDFSfilename, fileHash.String())

	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)
	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	fileData, err := os.ReadFile(localFilename)
	if err != nil {
		ConsolePrintf("Error reading file %s: %v\n", localFilename, err)
		return
	}

	ConsolePrintf("Read file %s (%d bytes)\n", localFilename, len(fileData))

	appendContentHash := fmt.Sprintf("%x", sha256.Sum256(fileData))
	clientTimestamp := fmt.Sprintf("%d_%s", time.Now().Unix(), AddressToVMName[s.Addr])

	appendInfo := AppendInfo{
		FileName:        hyDFSfilename,
		AppendHash:      appendContentHash,
		AppendID:        fmt.Sprintf("%s_%s", hyDFSfilename, clientTimestamp),
		ClientTimestamp: clientTimestamp,
		ClientID:        s.ID(),
		Size:            int64(len(fileData)),
	}

	ConsolePrintf("Created append metadata: %+v\n", appendInfo)

	// 5. Send file to all alive replicas (concurrently)
	type result struct {
		err error
	}
	done := make(chan result, len(targetMembers))

	for _, targetMember := range targetMembers {
		targetAddr := targetMember.Address
		go func(addr string) {
			// Each replica receives the file + append metadata
			err := s.SendAppendToNode(addr, fileData, appendInfo)
			done <- result{err: err}
		}(targetAddr)
	}

	ConsolePrintf("Append operation completed successfully for %s\n", hyDFSfilename)
}

// handleMergeCommand processes the merge command from CLI
func (s *Server) handleMergeCommand(hyDFSfilename string) {
	// Hash the filename to get its hash value
	fileHash := HashToMbits(hyDFSfilename)
	ConsolePrintf("[handleMergeCommand] File hash for %s: %s\n", hyDFSfilename, fileHash.String())

	// Get current node's primary key range
	start, end := GetPrimaryKeyRange(s.Members.GetAll(), s.ID())
	if start == nil || end == nil {
		ConsolePrintf("[handleMergeCommand] Could not determine primary key range\n")
		return
	}

	// Check if file hash is within the current node's primary range
	fileHashPtr := &fileHash
	inRange := IsHashInRange(fileHashPtr, start, end)

	if inRange {
		ConsolePrintf("[handleMergeCommand] File %s is in current node's primary range, executing merge\n", hyDFSfilename)
		s.executeMerge(hyDFSfilename)
	} else {
		ConsolePrintf("[handleMergeCommand] File %s is not in current node's primary range (range: %s - %s)\n",
			hyDFSfilename, start.String(), end.String())
		ConsolePrintf("[handleMergeCommand] Finding primary node for this file\n")

		// Find target nodes for the file hash
		targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)
		if len(targetMembers) == 0 {
			ConsolePrintf("[handleMergeCommand] No suitable nodes found for file %s\n", hyDFSfilename)
			return
		}

		// Primary node is the first target member
		primaryNode := targetMembers[0]
		ConsolePrintf("[handleMergeCommand] Primary node for file %s: %s (%s)\n",
			hyDFSfilename, primaryNode.ID(), primaryNode.Address)

		// Send RPC to primary node to execute merge
		resp, err := CallMerge(primaryNode.Address, hyDFSfilename, s)
		if err != nil {
			ConsolePrintf("[handleMergeCommand] Failed to send merge RPC to primary node %s: %v\n",
				primaryNode.ID(), err)
		} else if !resp.Success {
			ConsolePrintf("[handleMergeCommand] Merge operation failed on primary node %s: %s\n",
				primaryNode.ID(), resp.Message)
		} else {
			ConsolePrintf("[handleMergeCommand] Merge operation completed on primary node %s: %s\n",
				primaryNode.ID(), resp.Message)
		}
	}
}

func (s *Server) handleMultiAppend(hyDFSfilename string, vmNames []string, localFiles []string) {
	// Send multiappend RPC calls to VMs in parallel
	done := make(chan struct {
		vmName string
		err    error
	}, len(vmNames))

	for i := 0; i < len(vmNames); i++ {
		vmName := vmNames[i]
		localFile := localFiles[i]

		// Get VM address from NodeMap
		vmAddress, ok := NodeMap[vmName]
		if !ok {
			ConsolePrintf("Invalid VM name: %s (not found in NodeMap)\n", vmName)
			done <- struct {
				vmName string
				err    error
			}{vmName: vmName, err: fmt.Errorf("VM %s not found in NodeMap", vmName)}
			continue
		}

		// Call RPC in parallel using goroutine
		go func(addr, localFile, hyDFSFile string, vm string) {
			resp, err := CallMultiAppend(addr, hyDFSFile, localFile, s)
			if err != nil {
				ConsolePrintf("MultiAppend failed for VM %s (%s): %v\n", vm, addr, err)
				done <- struct {
					vmName string
					err    error
				}{vmName: vm, err: err}
			} else {
				ConsolePrintf("MultiAppend succeeded for VM %s (%s): %s\n", vm, addr, resp.Message)
				done <- struct {
					vmName string
					err    error
				}{vmName: vm, err: nil}
			}
		}(vmAddress, localFile, hyDFSfilename, vmName)
	}

	// Wait for all RPC calls to complete
	successCount := 0
	for i := 0; i < len(vmNames); i++ {
		result := <-done
		if result.err == nil {
			successCount++
		}
	}

	ConsolePrintf("MultiAppend completed: %d/%d successful\n", successCount, len(vmNames))
}

// findTargetNodes finds the primary node and replicas for a given file hash
func (s *Server) findTargetNodes(fileHash big.Int, replicationFactor int) []Member {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return []Member{}
	}
	fileHashPtr := new(big.Int).Set(&fileHash)

	// Find primary node (first node with hash >= file hash)
	primaryIndex := -1
	for i, member := range members {
		if member.Status != StatusAlive {
			continue
		}

		memberHashInt := member.Hash

		if memberHashInt.Cmp(fileHashPtr) >= 0 {
			primaryIndex = i
			break
		}
	}

	// If no node found with hash >= file hash, wrap around to first node
	if primaryIndex == -1 {
		for i, member := range members {
			if member.Status == StatusAlive {
				primaryIndex = i
				break
			}
		}
	}

	if primaryIndex == -1 {
		return []Member{}
	}

	// Get primary node and replicas
	var targetMembers []Member
	for i := 0; i < replicationFactor; i++ {
		index := (primaryIndex + i) % len(members)
		if members[index].Status == StatusAlive {
			targetMembers = append(targetMembers, members[index])
		}
	}

	return targetMembers
}

func (s *Server) executeMerge(hyDFSfilename ...string) {
	// Get primary key range
	start, end := GetPrimaryKeyRange(s.Members.GetAll(), s.ID())
	if start == nil || end == nil {
		ConsolePrintf("[handleMerge] Could not determine primary key range\n")
		return
	}

	// Get files in primary key range
	allFiles := GetFilesWithinRange(s.Metadata.Files, start, end)
	ConsolePrintf("[handleMerge] Found %d files in primary key range\n", len(allFiles))

	// If filename is provided, check if it's in allFiles and filter to only that file
	if len(hyDFSfilename) > 0 && hyDFSfilename[0] != "" {
		filename := hyDFSfilename[0]
		found := false
		for _, f := range allFiles {
			if f == filename {
				found = true
				break
			}
		}
		if found {
			allFiles = []string{filename}
			ConsolePrintf("[handleMerge] Processing single file: %s\n", filename)
		} else {
			ConsolePrintf("[handleMerge] File %s not found in primary key range, processing all files\n", filename)
		}
	}

	// Get successors
	successors := GetSuccessors(s.Members.GetAll(), s.ID(), Config.ReplicationFactor)
	if len(successors) == 0 {
		ConsolePrintf("[handleMerge] No successors found\n")
		return
	}

	// For each file in primary range, ensure append order matches on all successors
	for _, filename := range allFiles {
		// Get local metadata (source of truth)
		localMeta, ok := s.Metadata.GetFile(filename)
		if !ok {
			ConsolePrintf("[handleMerge] File %s not found locally, skipping\n", filename)
			continue
		}

		localAppendIDs := make([]string, len(localMeta.Appends))
		for i, app := range localMeta.Appends {
			localAppendIDs[i] = app.AppendID
		}

		// For each successor, check and update append order
		for _, successor := range successors {
			// Fetch metadata from successor
			succMeta := s.fetchMetadataFromNode(successor, *start, *end)
			remoteMeta, exists := succMeta.Files[filename]

			if !exists {
				ConsolePrintf("[handleMerge] File %s not found on successor %s, skipping order check\n", filename, successor.ID())
				continue
			}

			// Compare append order
			remoteAppendIDs := make([]string, len(remoteMeta.Appends))
			for i, app := range remoteMeta.Appends {
				remoteAppendIDs[i] = app.AppendID
			}

			// Check if order is identical
			orderMatches := len(localAppendIDs) == len(remoteAppendIDs)
			if orderMatches {
				for i := 0; i < len(localAppendIDs); i++ {
					if localAppendIDs[i] != remoteAppendIDs[i] {
						orderMatches = false
						break
					}
				}
			}

			if !orderMatches {
				ConsolePrintf("[handleMerge] Append order differs for file %s on successor %s, updating...\n", filename, successor.ID())
				ConsolePrintf("[handleMerge] Local order: %v\n", localAppendIDs)
				ConsolePrintf("[handleMerge] Remote order: %v\n", remoteAppendIDs)

				// Update append order on successor
				resp, err := CallUpdateAppendOrder(successor.Address, filename, localMeta.Appends, s)
				if err != nil {
					ConsolePrintf("[handleMerge] Failed to update append order on %s: %v\n", successor.ID(), err)
				} else if !resp.Success {
					ConsolePrintf("[handleMerge] Update append order failed on %s: %s\n", successor.ID(), resp.Message)
				} else {
					ConsolePrintf("[handleMerge] Successfully updated append order for file %s on %s\n", filename, successor.ID())
				}
			} else {
				ConsolePrintf("[handleMerge] Append order matches for file %s on successor %s\n", filename, successor.ID())
			}
		}
	}

	ConsolePrintf("[handleMerge] Merge operation completed\n")
}
