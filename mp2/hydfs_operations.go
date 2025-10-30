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

	// Send file to target nodes
	//TODO : Send file to all replicas that are ALIVE but wait for only 1 response(W = 1, R = 1). Use go-routines
	//TODO : If all replicas are dead, FAIL the operation.
	//TODO: During read / write, only wait for one replica's response. Merge will guarantee eventual consistency.
	for _, targetMember := range targetMembers {
		ConsolePrintf("Sending file to %s...\n", convertToGRPCAddress(targetMember.Address))
		if err := s.SendFileToNode(targetMember.Address, fileData, fileMetadata); err != nil {
			ConsolePrintf("Error sending file to %s: %v\n", convertToGRPCAddress(targetMember.Address), err)
		}
	}

	ConsolePrintf("File creation completed: %s -> %s\n", localFilename, hyDFSfilename)
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
	clientTimestamp := fmt.Sprintf("%d", time.Now().Unix())

	appendInfo := AppendInfo{
		FileName:        hyDFSfilename,
		AppendHash:      appendContentHash,
		AppendID:        fmt.Sprintf("%s_%s_%s", hyDFSfilename, clientTimestamp, s.ID()),
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

//TODO: Implement get, merge and append for HyDFS
