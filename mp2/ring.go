package main

import (
	"math/big"
	"os"
)

// GetNodeIndex returns the index of nodeID in a member slice
func GetNodeIndex(members []Member, nodeID string) int {
	for i, m := range members {
		if m.ID() == nodeID {
			return i
		}
	}
	return -1
}

// GetRingSuccessorIdx returns the index for the (idx+step)th node in the ring, wrapping
func GetRingSuccessorIdx(idx, step, n int) int {
	return (idx + step) % n
}

// GetRingPredecessorIdx returns the predecessor index for idx in the ring
func GetRingPredecessorIdx(idx, n int) int {
	return (idx - 1 + n) % n
}

// GetPrimaryKeyRange returns the predecessor and own hash for a node
func GetPrimaryKeyRange(members []Member, nodeID string) (start, end *big.Int) {
	N := len(members)
	myIdx := GetNodeIndex(members, nodeID)
	if N == 0 || myIdx == -1 {
		return nil, nil
	}
	predIdx := GetRingPredecessorIdx(myIdx, N)
	predHash := &members[predIdx].Hash
	myHash := &members[myIdx].Hash
	return predHash, myHash
}

// GetFilesWithinRange returns filenames whose hashes are in (start, end]
func GetFilesWithinRange(files map[string]FileMetadata, startRange, endRange *big.Int) []string {
	var result []string
	for name, meta := range files {
		fileHashPtr := &meta.FileNameHash
		cmpStart := fileHashPtr.Cmp(startRange)
		cmpEnd := fileHashPtr.Cmp(endRange)
		if startRange.Cmp(endRange) < 0 {
			if cmpStart > 0 && cmpEnd <= 0 {
				result = append(result, name)
			}
		} else {
			if cmpStart > 0 || cmpEnd <= 0 {
				result = append(result, name)
			}
		}
	}
	return result
}

// GetSuccessors returns the next (n-1) alive successors after nodeID in sorted/liveness-filtered members
func GetSuccessors(members []Member, nodeID string, n int) []Member {
	myIdx := GetNodeIndex(members, nodeID)
	if myIdx == -1 || len(members) == 0 {
		return nil
	}
	succ := []Member{}
	count := 0
	for i := 1; count < n-1 && i < len(members); i++ {
		idx := GetRingSuccessorIdx(myIdx, i, len(members))
		if members[idx].IsAlive() {
			succ = append(succ, members[idx])
			count++
		}
	}
	return succ
}

// TODO: Test this and fix using logs
func (s *Server) stabilizeRing() {
	start, end := GetPrimaryKeyRange(s.Members.GetAll(), s.ID())
	if start == nil || end == nil {
		return
	}
	myFiles := GetFilesWithinRange(s.Metadata.Files, start, end)
	successors := GetSuccessors(s.Members.GetAll(), s.ID(), Config.ReplicationFactor)

	for _, successor := range successors {
		succMeta := s.fetchMetadataFromNode(successor, *start, *end)
		for _, filename := range myFiles {
			localMeta, ok := s.Metadata.GetFile(filename)
			if !ok {
				continue // file locally deleted
			}
			remoteMeta, exists := succMeta.Files[filename]
			// 1. Forward missing or outdated file
			if !exists || localMeta.FileContentHash != remoteMeta.FileContentHash {
				ConsolePrintf("[stabilizeRing] File '%s' is missing or outdated on %s: transferring.\n", filename, successor.Address)
				data, err := os.ReadFile(s.FileDirectory + filename)
				if err == nil {
					// TODO: Send local metadata but appends should be empty
					s.SendFileToNode(successor.Address, data, localMeta)
				} else {
					ConsolePrintf("[stabilizeRing] WARN: Could not find file data for transfer: %s\n", filename)
				}
			}

			// 2. Forward missing appends (local appends not in remote)
			appends := localMeta.Appends
			remoteAppends := map[string]bool{}
			if exists {
				for _, app := range remoteMeta.Appends {
					remoteAppends[app.AppendID] = true
				}
			}
			for _, app := range appends {
				if !remoteAppends[app.AppendID] {
					data, err := os.ReadFile(s.FileDirectory + app.AppendID)
					if err == nil {
						// TODO: Send local metadata but appends should be empty
						s.SendAppendToNode(successor.Address, data, app)
					} else {
						ConsolePrintf("[stabilizeRing] WARN: No append data for %s on file %s, skipping copy.\n", app.AppendID, filename)
					}
				}
			}
		}
	}
}

// TODO: Revisit and review this logic on when to trigger --> Current logic is not correct.
func (s *Server) handleReplicationWindowChange(nodeID string) {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return
	}
	changedIdx := GetNodeIndex(members, nodeID)
	myIdx := GetNodeIndex(members, s.ID())
	if changedIdx == -1 || myIdx == -1 {
		ConsolePrintf("Could not find node in ring for stabilization check\n")
		return
	}
	for i := 1; i < Config.ReplicationFactor; i++ {
		if GetRingSuccessorIdx(myIdx, i, len(members)) == changedIdx {
			ConsolePrintf("Node %s in my replication set; triggering stabilization\n", nodeID)
			s.stabilizeRing()
			return
		}
	}
	ConsolePrintf("Node %s is not in my replication set; no stabilization needed\n", nodeID)
}
