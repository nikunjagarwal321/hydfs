package main

import (
	"fmt"
	"time"
)

type Server struct {
	Addr                  string
	NodeCreationTimestamp time.Time
	IntroducerAddr        string
	IncarnationNumber     uint64
	HeartbeatCounter      uint64
	IsIntroducer          bool
	Members               *MembershipList
}

func (s *Server) ID() string {
	return fmt.Sprintf("%s-%d", s.Addr, s.NodeCreationTimestamp.UnixNano())
}

// IF ANY GLOBAL PROPERTY IS RELATED TO SERVER, SET IT HERE
func NewServer(addr, introducerAddr string, isIntroducer bool) *Server {
	// Adding itself in the membership based on the example in class
	membershipList := NewMembershipList()
	member := Member{
		Address:               addr,
		NodeCreationTimestamp: time.Now(),
		Status:                StatusAlive,
		Heartbeat:             1,
		Incarnation:           1,
		LastUpdated:           time.Now(),
	}
	membershipList.AddOrUpdate(member)

	return &Server{
		Addr:                  addr,
		NodeCreationTimestamp: member.NodeCreationTimestamp,
		IntroducerAddr:        introducerAddr,
		IncarnationNumber:     member.Incarnation,
		HeartbeatCounter:      member.Heartbeat,
		IsIntroducer:          isIntroducer,
		Members:               membershipList,
	}
}

func (s *Server) Start() error {
	return s.StartRPCServer()
}

// Checks for suspicion nodes
func (s *Server) backgroundCheckerRPC(interval, suspicionTimeout, deadTimeout, cleanUpTimeout time.Duration) {
	//  TODO: Only use suspicion when it is enabled
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		changed := s.Members.MarkSuspectIfNeeded(suspicionTimeout, deadTimeout, cleanUpTimeout)
		if changed {
			s.Members.Print()
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
