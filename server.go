package main

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Global server instance (will be set in main)
var globalServer *Server

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
	LocalDirectory                  string
	FailedPendingStabilization      map[string]StabilizationStatus
	NewlyJoinedPendingStabilization map[string]StabilizationStatus
	stabilizationLock               sync.Mutex
}

func (s *Server) ID() string {
	return fmt.Sprintf("%s-%d", s.Addr, s.NodeCreationTimestamp.Unix())
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

	// Get VM name from reverse map for FileDirectory
	vmName := AddressToVMName[addr]

	// Cleanup own directory during startup
	if vmName != "" {
		nodeDir := filepath.Join("../hydfs", vmName)
		if err := os.RemoveAll(nodeDir); err != nil {
			if !os.IsNotExist(err) {
				ConsolePrintf("[NewServer] Failed to cleanup own directory %s: %v\n", nodeDir, err)
			}
		} else {
			LogInfo(true, "[NewServer] Cleaned up own directory %s\n", nodeDir)
		}
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
		FileDirectory:                   "../hydfs/" + vmName,
		LocalDirectory:                  "../../localdirectory/" + vmName,
		FailedPendingStabilization:      make(map[string]StabilizationStatus),
		NewlyJoinedPendingStabilization: make(map[string]StabilizationStatus),
		stabilizationLock:               sync.Mutex{},
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

func (s *Server) garbageCollectorProcess(garbageCollectorInterval time.Duration) {
	ticker := time.NewTicker(garbageCollectorInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.deleteFilesOutsideMyRange()
	}
}

func (s *Server) mergeMetadataProcess(mergeInterval time.Duration) {
	ticker := time.NewTicker(mergeInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.executeMergeForAllFiles()
	}
}
