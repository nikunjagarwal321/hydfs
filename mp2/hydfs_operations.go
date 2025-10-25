package main

import (
	"math/big"
	"os"
)

func (s *Server) handleCreate(localFilename, hyDFSfilename string) {
	// Hash the HyDFS filename to determine target nodes
	fileHash := HashToMbits(hyDFSfilename).String()
	ConsolePrintf("File hash for %s: %s\n", hyDFSfilename, fileHash)

	// Find target nodes (primary + replicas)
	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)

	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	// Display target nodes
	ConsolePrintf("Primary node: %s\n", convertToGRPCAddress(targetMembers[0].Address))
	if len(targetMembers) > 1 {
		ConsolePrintf("Replica nodes: ")
		for i := 1; i < len(targetMembers); i++ {
			ConsolePrintf("%s ", convertToGRPCAddress(targetMembers[i].Address))
		}
		ConsolePrintf("\n")
	}

	// Read local file
	fileData, err := os.ReadFile(localFilename)
	if err != nil {
		ConsolePrintf("Error reading file %s: %v\n", localFilename, err)
		return
	}

	ConsolePrintf("Read file %s (%d bytes)\n", localFilename, len(fileData))

	// Send file to target nodes
	//TODO : Send file to all replicas that are ALIVE but wait for only 1 response(W = 1, R = 1). Use go-routines
	//TODO : If all replicas are dead, FAIL the operation.
	//TODO: During read / write, only wait for one replica's response. Merge will guarantee eventual consistency.
	for _, targetMember := range targetMembers {
		ConsolePrintf("Sending file to %s...\n", convertToGRPCAddress(targetMember.Address))
		if err := s.SendFileToNode(targetMember.Address, hyDFSfilename, fileData); err != nil {
			ConsolePrintf("Error sending file to %s: %v\n", convertToGRPCAddress(targetMember.Address), err)
		}
	}

	ConsolePrintf("File creation completed: %s -> %s\n", localFilename, hyDFSfilename)
}

// findTargetNodes finds the primary node and replicas for a given file hash
func (s *Server) findTargetNodes(fileHash string, replicationFactor int) []Member {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return []Member{}
	}

	// Convert file hash to big.Int for comparison
	fileHashInt, ok := new(big.Int).SetString(fileHash, 10)
	if !ok {
		ConsolePrintf("Invalid file hash: %s\n", fileHash)
		return []Member{}
	}

	// Find primary node (first node with hash >= file hash)
	primaryIndex := -1
	for i, member := range members {
		if member.Status != StatusAlive {
			continue
		}

		memberHashInt, ok := new(big.Int).SetString(member.Hash, 10)
		if !ok {
			continue // Skip members with invalid hash
		}

		if memberHashInt.Cmp(fileHashInt) >= 0 {
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
