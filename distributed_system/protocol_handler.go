package main

import (
	"fmt"
	"math/rand"
	"time"
)

// GossipMessage carries membership updates
type GossipMessage struct {
	SenderAddress  string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

// Global server instance (will be set in main)
var globalServer *Server

// Global variables for round-robin node selection
var (
	nodeOrder       []string // Ordered list of node addresses
	currentPointer  int      // Current position in the order
	lastMemberCount int      // To detect membership changes
)

func selectNextKNodes(k int, self string, membershipSnapshot map[string]Member) []string {
	// Build candidates list
	var candidates []string
	for nodeId, member := range membershipSnapshot {
		if nodeId != self && member.Status != StatusDead {
			candidates = append(candidates, member.Address)
		}
	}

	if len(candidates) == 0 {
		return []string{}
	}

	// Rebuild order if membership changed
	if len(candidates) != lastMemberCount || len(nodeOrder) == 0 {
		nodeOrder = candidates
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(nodeOrder), func(i, j int) {
			nodeOrder[i], nodeOrder[j] = nodeOrder[j], nodeOrder[i]
		})
		currentPointer = 0
		lastMemberCount = len(candidates)
	}

	// Select next k nodes
	if k > len(nodeOrder) {
		k = len(nodeOrder)
	}

	result := make([]string, k)
	for i := 0; i < k; i++ {
		result[i] = nodeOrder[currentPointer]
		currentPointer = (currentPointer + 1) % len(nodeOrder)
	}

	return result
}

// Send gossip to random nodes
func (s *Server) gossipSend(nodeCount int) {

	snapshot := s.Members.Snapshot()
	nodes := selectNextKNodes(nodeCount, s.ID(), snapshot)

	if len(nodes) == 0 {
		return // No nodes to gossip to
	}

	membersList := []Member{}
	for _, m := range snapshot {
		membersList = append(membersList, m)
	}

	for _, node := range nodes {
		go func(nodeAddr string) {
			resp, err := CallGossip(nodeAddr, s.ID(), membersList)
			if err != nil {
				fmt.Printf("Failed to send gossip to %s: %v\n", nodeAddr, err)
			} else {
				fmt.Printf("Sent gossip to %s, success: %v\n", nodeAddr, resp.Success)
			}
		}(node)
	}
}

// Send ping to random nodes
func (s *Server) pingSend(nodeCount int) {

	// TODO: In future, send partial membership list rather than full list

	snapshot := s.Members.Snapshot()
	nodes := selectNextKNodes(nodeCount, s.ID(), snapshot)

	if len(nodes) == 0 {
		return // No nodes to gossip to
	}

	membersList := []Member{}
	for _, m := range snapshot {
		membersList = append(membersList, m)
	}

	for _, node := range nodes {
		go func(nodeAddr string) {
			resp, err := CallPing(nodeAddr, s.ID(), membersList)
			if err != nil {
				fmt.Printf("Failed to send gossip to %s: %v\n", nodeAddr, err)
			} else {
				mergeMembership(s, resp.MembershipList)
				fmt.Printf("Sent gossip to %s\n", nodeAddr)
			}
		}(node)
	}
}

// mergeMembership merges the received membership list with local membership
func mergeMembership(server *Server, receivedMembers []Member) {
	localSnapshot := server.Members.Snapshot()

	for _, receivedMember := range receivedMembers {
		_, exists := localSnapshot[receivedMember.ID()]

		if !exists {
			// New member, add it
			receivedMember.LastUpdated = time.Now()
			server.Members.AddOrUpdate(receivedMember)
			fmt.Printf("Added new member: %s\n", receivedMember.ID())
			continue
		}

		localMember := localSnapshot[receivedMember.ID()]

		// Move this to a diff function
		// Special case: Handle self-node with suspicion enabled
		if Config.Suspicion == Suspect && receivedMember.Address == server.Addr {
			if receivedMember.Status == StatusSuspect && localMember.Status == StatusAlive {
				// We are alive but others think we are suspect - increment incarnation
				server.IncarnationNumber++
				localMember.Incarnation = server.IncarnationNumber
				localMember.LastUpdated = time.Now()
				server.Members.AddOrUpdate(localMember)
				fmt.Printf("Incremented incarnation to %d - I'm alive but was marked as suspect\n", server.IncarnationNumber)
				continue
			}
			if receivedMember.Status == StatusDead {
				// If others think we are dead, do nothing (we know we are alive)
				fmt.Printf("Ignoring dead status for self from external source\n")
				continue
			}
		}

		var updatedMember Member

		switch Config.Suspicion {
		case Suspect:
			updatedMember = handleSuspicionMerge(localMember, receivedMember)
		case NoSuspect:
			updatedMember = handleNoSuspicionMerge(localMember, receivedMember)
		}

		server.Members.AddOrUpdate(updatedMember)
		fmt.Printf("Processed member: %s\n", updatedMember.ID())

	}

}

// if node is self, and status is suspect --> Treat differently. Change own status to alive and increase the incarnation number. Do this is separate function
// If node is self and status is failed --> do nothing
// Failed overrides everything, even incarnation number
// Then, Incarnation number always takes priority in both the protocols
// If Incarnation number is same, Suspect > Alive
// If Incarnation number is also the same and status is also the same, then for Gossip --> Heartbeat counter takes priority
func handleSuspicionMerge(localMember Member, receivedMember Member) Member {
	// Rule 1: Dead/VoluntaryLeave always wins (overrides everything, even incarnation number)
	if receivedMember.Status == StatusDead || receivedMember.Status == StatusVoluntaryLeave {
		receivedMember.LastUpdated = time.Now()
		return receivedMember
	}
	if localMember.Status == StatusDead || localMember.Status == StatusVoluntaryLeave {
		return localMember // Keep local final status
	}

	// Rule 2: Higher incarnation number always takes priority
	if receivedMember.Incarnation > localMember.Incarnation {
		receivedMember.LastUpdated = time.Now()
		return receivedMember
	}
	if localMember.Incarnation > receivedMember.Incarnation {
		return localMember // Keep local (higher incarnation)
	}

	// Rule 3: Same incarnation number - status priority (Suspect > Alive)
	if receivedMember.Incarnation == localMember.Incarnation {
		// If both have same status and same incarnation
		if receivedMember.Status == localMember.Status {
			// For same status and incarnation, protocol-specific logic
			switch Config.Protocol {
			case GossipProtocol:
				// For gossip: higher heartbeat wins
				if receivedMember.Heartbeat > localMember.Heartbeat {
					receivedMember.LastUpdated = time.Now()
					return receivedMember
				}
				return localMember
			case PingAckProtocol:
				// For PingAck: just take received (more recent information)
				receivedMember.LastUpdated = time.Now()
				return receivedMember
			}
		} else {
			// Different status, same incarnation: Suspect > Alive
			if compareStatus(receivedMember.Status, localMember.Status) {
				receivedMember.LastUpdated = time.Now()
				return receivedMember
			}
			return localMember
		}
	}

	// Default: keep local
	return localMember
}

func handleNoSuspicionMerge(localMember Member, receivedMember Member) Member {
	// TODO: verify what will happen, if self node is dead
	switch Config.Protocol {
	case GossipProtocol:
		// For gossip without suspicion: only return if higher heartbeat
		if receivedMember.Heartbeat <= localMember.Heartbeat {
			return localMember
		}
	case PingAckProtocol:
		// TODO : confirm this with prof
	}
	receivedMember.LastUpdated = time.Now()
	return receivedMember
}

// compareStatus returns true if received should replace local
func compareStatus(received, local Status) bool {
	return getStatusPriority(received) > getStatusPriority(local)
}

// getStatusPriority: Dead = VoluntaryLeave > Suspect > Alive
func getStatusPriority(status Status) int {
	switch status {
	case StatusDead, StatusVoluntaryLeave:
		return 3 // Both dead and voluntary leave have highest priority
	case StatusSuspect:
		return 2
	case StatusAlive:
		return 1
	default:
		return 0
	}
}
