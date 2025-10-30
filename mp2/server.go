package main

import (
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"
)

// Global server instance (will be set in main)
var globalServer *Server

// BandwidthStats tracks network bandwidth usage
type BandwidthStats struct {
	BytesSent     uint64
	BytesReceived uint64
	mu            sync.RWMutex
}

func (bs *BandwidthStats) AddSent(bytes uint64) {
	bs.mu.Lock()
	bs.BytesSent += bytes
	bs.mu.Unlock()
}

func (bs *BandwidthStats) AddReceived(bytes uint64) {
	bs.mu.Lock()
	bs.BytesReceived += bytes
	bs.mu.Unlock()
}

func (bs *BandwidthStats) GetAndReset() (sent, received uint64) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	sent = bs.BytesSent
	received = bs.BytesReceived
	bs.BytesSent = 0
	bs.BytesReceived = 0
	return
}

type StabilizationStatus string

const (
	NewlyJoined    StabilizationStatus = "NEWLY_JOINED"
	FailedNode     StabilizationStatus = "FAILED_NODE"
	ProcessedNode  StabilizationStatus = "PROCESSED_NODE"
	InProgressNode StabilizationStatus = "IN_PROGRESS_NODE"
)

type Server struct {
	Addr                            string
	NodeCreationTimestamp           time.Time
	IntroducerAddr                  string
	IncarnationNumber               uint64
	HeartbeatCounter                uint64
	IsIntroducer                    bool
	Members                         *MembershipList
	BandwidthStats                  *BandwidthStats
	Hash                            big.Int
	Metadata                        *Metadata
	FileDirectory                   string
	FailedPendingStabilization      map[string]StabilizationStatus
	NewlyJoinedPendingStabilization map[string]StabilizationStatus
}

func (s *Server) ID() string {
	return fmt.Sprintf("%s-%d", s.Addr, s.NodeCreationTimestamp.UnixNano())
}

// IF ANY GLOBAL PROPERTY IS RELATED TO SERVER, SET IT HERE
func NewServer(addr, introducerAddr string, isIntroducer bool) *Server {
	// Adding itself in the membership based on the example in class
	membershipList := NewMembershipList()

	// Compute hash for this server
	hashValue := HashToMbits(addr)

	member := Member{
		Address:               addr,
		NodeCreationTimestamp: time.Now(),
		Status:                StatusAlive,
		Heartbeat:             1,
		Incarnation:           1,
		LastUpdated:           time.Now(),
		Hash:                  hashValue,
	}
	membershipList.AddOrUpdate(member)

	metadata := &Metadata{
		Files: make(map[string]FileMetadata),
	}

	return &Server{
		Addr:                            addr,
		NodeCreationTimestamp:           member.NodeCreationTimestamp,
		IntroducerAddr:                  introducerAddr,
		IncarnationNumber:               member.Incarnation,
		HeartbeatCounter:                member.Heartbeat,
		IsIntroducer:                    isIntroducer,
		Members:                         membershipList,
		BandwidthStats:                  &BandwidthStats{},
		Hash:                            hashValue,
		Metadata:                        metadata,
		FileDirectory:                   "hydfs_" + addr,
		FailedPendingStabilization:      make(map[string]StabilizationStatus),
		NewlyJoinedPendingStabilization: make(map[string]StabilizationStatus),
	}
}

func (s *Server) Start() error {
	return s.StartRPCServer()
}

// Checks for suspicion nodes
func (s *Server) backgroundCheckerRPC(interval, suspicionTimeout, deadTimeout, cleanUpTimeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		changed := s.Members.MarkSuspectIfNeeded(suspicionTimeout, deadTimeout, cleanUpTimeout)
		if changed {
			s.Members.Print(false)
		}

	}
}

// Calls other servers based on GOSSIP or SWIM protocol
func (s *Server) sendTimelyMessagesAsPerProtocol(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		switch Config.Protocol {
		case GossipProtocol:
			s.gossipSend(Config.Fanout)
		case PingAckProtocol:
			s.pingSend(Config.Fanout)
		}
	}
}

func (s *Server) stabilizeUpdatedNodes(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		for nodeID, status := range s.FailedPendingStabilization {
			if status == FailedNode {
				s.FailedPendingStabilization[nodeID] = InProgressNode
				s.handleFailedNode(nodeID)
			}
		}
		for nodeID, status := range s.NewlyJoinedPendingStabilization {
			if status == NewlyJoined {
				s.NewlyJoinedPendingStabilization[nodeID] = InProgressNode
				s.handleNewlyJoinedNode(nodeID)
			}
		}
	}
}

// Returns the start and end of the key range (hashes) for which this node is primary
func (s *Server) getPrimaryKeyRange() (start, end *big.Int) {
	members := s.Members.GetAll()
	N := len(members)
	if N == 0 {
		return nil, nil
	}
	myHash := &s.Hash
	myIdx := -1
	for i, m := range members {
		if m.Hash.Cmp(myHash) == 0 && m.IsAlive() {
			myIdx = i
			break
		}
	}
	if myIdx == -1 {
		return nil, nil
	}
	predIdx := (myIdx - 1 + N) % N
	predHash := &members[predIdx].Hash
	return predHash, myHash
}

// Returns all file names in s.Metadata.Files that are in (startRange, endRange] in the ring (wrap supported)
func (s *Server) getFilesWithinRange(startRange, endRange *big.Int) []string {
	var result []string
	for name, meta := range s.Metadata.Files {
		fileHashPtr := &meta.FileNameHash
		cmpStart := fileHashPtr.Cmp(startRange)
		cmpEnd := fileHashPtr.Cmp(endRange)
		if startRange.Cmp(endRange) < 0 {
			// Normal: (start, end]
			if cmpStart > 0 && cmpEnd <= 0 {
				result = append(result, name)
			}
		} else {
			// Ring wrap: (start, max] U [min, end]
			if cmpStart > 0 || cmpEnd <= 0 {
				result = append(result, name)
			}
		}
	}
	return result
}

// Returns the list of successor Members for this node (for replication)
func (s *Server) getSuccessors() []Member {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return nil
	}
	myIdx := -1
	for i, m := range members {
		if m.Hash.Cmp(&s.Hash) == 0 && m.IsAlive() {
			myIdx = i
			break
		}
	}
	if myIdx == -1 {
		return nil
	}

	succ := []Member{}
	count := 0
	for i := 1; count < Config.ReplicationFactor-1 && i < len(members); i++ {
		idx := (myIdx + i) % len(members)
		if members[idx].IsAlive() {
			succ = append(succ, members[idx])
			count++
		}
	}
	return succ
}

// fetchMetadataFromNode fetches the file metadata for the range (or file) from a remote node
func (s *Server) fetchMetadataFromNode(node Member, start, end big.Int) Metadata {
	meta, err := GetKeysMetadata(node.Address, globalServer, start, end)
	if err != nil || meta == nil {
		return Metadata{Files: make(map[string]FileMetadata)}
	}
	return meta.Metadata
}

func (s *Server) stabilizeRing() {
	start, end := s.getPrimaryKeyRange()
	myFiles := s.getFilesWithinRange(start, end)
	successors := s.getSuccessors()

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
					fileData := loadAppendData(app)
					if fileData != nil {
						ConsolePrintf("[stabilizeRing] Forwarding missing append %s of file %s to %s\n", app.AppendID, filename, successor.Address)
						s.SendAppendToNode(successor.Address, fileData, app)
					} else {
						ConsolePrintf("[stabilizeRing] WARN: No append data for %s on file %s, skipping copy.\n", app.AppendID, filename)
					}
				}
			}
		}
	}
}

// stub for loading append data, modify as needed
func loadAppendData(app AppendInfo) []byte {
	// Example: file is named <filename>.<appendID> as in server, or stored elsewhere
	fname := fmt.Sprintf("hydfs/%s.%s", app.FileName, app.AppendID)
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil
	}
	return data
}

func (s *Server) handleFailedNode(nodeID string) {
	// TODO: Actual stabilization logic for failed nodes to be implemented as per requirements.
	s.stabilizeRing()
	ConsolePrintf("Handling stabilization for failed node: %s\n", nodeID)
}

func (s *Server) handleNewlyJoinedNode(nodeID string) {
	// TODO: Actual stabilization logic for newly joined nodes to be implemented as per requirements.
	ConsolePrintf("Handling stabilization for newly joined node: %s\n", nodeID)
}

// Monitors and displays bandwidth usage per second
func (s *Server) monitorBandwidth() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		sent, received := s.BandwidthStats.GetAndReset()
		if sent > 0 || received > 0 {
			LogInfo(true, "BANDWIDTH_%s_%s: Sent: %d bytes/s (%.2f KB/s), Received: %d bytes/s (%.2f KB/s), Total: %d bytes/s (%.2f KB/s),\n",
				Config.Protocol, Config.Suspicion, sent, float64(sent)/1024.0, received, float64(received)/1024.0, sent+received, float64(sent+received)/1024.0)
		}
	}
}

// increaseHeartbeat increments heartbeat counter for gossip protocol
func (s *Server) increaseHeartbeat(heartbeatInterval time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Increment the server's heartbeat counter only for Gossip
		if Config.Protocol == PingAckProtocol {
			continue
		}

		s.HeartbeatCounter++

		// Create updated member info for self
		selfMember := Member{
			Address:               s.Addr,
			NodeCreationTimestamp: s.NodeCreationTimestamp,
			Status:                StatusAlive,
			Heartbeat:             s.HeartbeatCounter,
			Incarnation:           s.IncarnationNumber,
			LastUpdated:           time.Now(),
			Hash:                  s.Hash, // Preserve the hash
		}

		// Update self in the membership list
		s.Members.AddOrUpdate(selfMember)

	}
}
