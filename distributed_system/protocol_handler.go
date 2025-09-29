package main

import (
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
			candidates = append(candidates, member.ID())
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

	// Create optimized membership list with only necessary fields
	membersList := []Member{}
	for _, m := range snapshot {
		// Create minimal member with only essential fields
		optimizedMember := Member{
			Address:               m.Address,               // Keep for ID generation
			NodeCreationTimestamp: m.NodeCreationTimestamp, // Keep for ID generation
			Status:                m.Status,                // Essential for merge logic
			Heartbeat:             m.Heartbeat,             // Used in gossip protocol
			Incarnation:           m.Incarnation,           // Essential for merge logic
			// Skip LastUpdated - gets set to time.Now() during merge
		}
		membersList = append(membersList, optimizedMember)
	}

	for _, node := range nodes {
		go func(nodeId string) {
			resp, err := CallGossip(GetAddressFromID(nodeId), s.ID(), membersList, s)
			if err != nil {
				LogError(true, "Failed to send gossip to %s: %v", nodeId, err)
			} else {
				LogInfo(false, "Sent gossip to %s, success: %v", nodeId, resp.Success)
			}
		}(node)
	}
}

// Send ping to random nodes
func (s *Server) pingSend(nodeCount int) {

	snapshot := s.Members.Snapshot()
	nodes := selectNextKNodes(nodeCount, s.ID(), snapshot)

	if len(nodes) == 0 {
		return // No nodes to gossip to
	}

	// Create optimized membership list with only necessary fields for PingAck
	membersList := []Member{}
	for _, m := range snapshot {
		// Create minimal member with only essential fields for PingAck protocol
		optimizedMember := Member{
			Address:               m.Address,               // Keep for ID generation
			NodeCreationTimestamp: m.NodeCreationTimestamp, // Keep for ID generation
			Status:                m.Status,                // Essential for merge logic
			Incarnation:           m.Incarnation,           // Essential for merge logic
			// Skip Heartbeat - not used in PingAck protocol
			// Skip LastUpdated - gets set to time.Now() during merge
		}
		membersList = append(membersList, optimizedMember)
	}

	for _, node := range nodes {
		go func(nodeId string) {
			resp, err := CallPing(GetAddressFromID(nodeId), s.ID(), membersList, s)
			if err != nil {
				LogError(true, "Failed to send ping to %s: %v", nodeId, err)
			} else {
				LogInfo(false, "PingAck: Received ACK from %s", nodeId)
				mergeMembership(s, resp.MembershipList, nodeId)
			}
		}(node)
	}
}

// mergeMembership merges the received membership list with local membership
func mergeMembership(server *Server, receivedMembers []Member, senderId string) {
	localSnapshot := server.Members.Snapshot()

	for _, receivedMember := range receivedMembers {

		if Config.Protocol == GossipProtocol && receivedMember.Status == StatusDead {
			LogInfo(false, "FAILURE_COMMUNICATED: Received dead node status from %s: %s during merge", senderId, receivedMember.ID())
			continue
		}

		// Skip processing if local member is already dead and received member is also dead
		_, exists := localSnapshot[receivedMember.ID()]

		if !exists && receivedMember.Status != StatusDead {
			// New member, add it
			receivedMember.LastUpdated = time.Now()
			server.Members.AddOrUpdate(receivedMember)
			LogInfo(true, "MEMBER_JOIN: Added new member: %s during merge", receivedMember.ID())
			continue
		} else if !exists && receivedMember.Status == StatusDead {
			continue // Handle edge case where a received dead node is not in local membership list in swim
		}

		localMember := localSnapshot[receivedMember.ID()]

		// TODO: Move this to a diff function to make more modular
		// Special case: Handle self-node with suspicion enabled
		if receivedMember.ID() == server.ID() {
			if receivedMember.Status == StatusSuspect && localMember.Status == StatusAlive &&
				receivedMember.Incarnation >= localMember.Incarnation {
				// We are alive but others think we are suspect - increment incarnation
				// TODO: Dont update everytime. Update only if local incarnation is less or equal
				server.IncarnationNumber++
				localMember.Incarnation = server.IncarnationNumber
				localMember.LastUpdated = time.Now()
				server.Members.AddOrUpdate(localMember)
				LogInfo(true, "Incremented incarnation to %d - I'm alive but was marked as suspect", server.IncarnationNumber)
				continue
			}
			if receivedMember.Status == StatusDead {
				// If others think we are dead, do nothing (we know we are alive)
				continue
			}
		}

		var updatedMember Member

		switch Config.Suspicion {
		case Suspect:
			updatedMember = handleSuspicionMerge(localMember, receivedMember, senderId)
		case NoSuspect:
			updatedMember = handleNoSuspicionMerge(localMember, receivedMember, senderId)
		}

		server.Members.AddOrUpdate(updatedMember)

	}

}

// if node is self, and status is suspect --> Treat differently. Change own status to alive and increase the incarnation number. Do this is separate function
// If node is self and status is failed --> do nothing
// Failed overrides everything, even incarnation number
// Then, Incarnation number always takes priority in both the protocols
// If Incarnation number is same, Suspect > Alive
// If Incarnation number is also the same and status is also the same, then for Gossip --> Heartbeat counter takes priority
func handleSuspicionMerge(localMember Member, receivedMember Member, receiverId string) Member {
	// Rule 1: Dead/VoluntaryLeave always wins (overrides everything, even incarnation number)
	if receivedMember.Status == StatusDead || receivedMember.Status == StatusVoluntaryLeave {
		// receivedMember.LastUpdated = time.Now()
		if localMember.Status != receivedMember.Status {
			LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed to %s (from %s) during merge", receivedMember.ID(), receivedMember.Status, localMember.Status)
		}
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
				if receivedMember.ID() == receiverId {
					receivedMember.LastUpdated = time.Now()
					return receivedMember
				}
				// For PingAck: just take received (more recent information)
				// TODO : CHECK, THIS MIGHT BE CAUSING THE ISSUE
			}
		} else {
			// Different status, same incarnation: Suspect > Alive
			if compareStatus(receivedMember.Status, localMember.Status) {
				LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed from %s to %s during merge", receivedMember.ID(), localMember.Status, receivedMember.Status)
				receivedMember.LastUpdated = time.Now()
				return receivedMember
			}
			return localMember
		}
	}

	// Default: keep local
	return localMember
}

func handleNoSuspicionMerge(localMember Member, receivedMember Member, receiverId string) Member {
	// TODO: verify what will happen, if self node is dead
	if receivedMember.Status == StatusDead || receivedMember.Status == StatusVoluntaryLeave {
		// receivedMember.LastUpdated = time.Now()
		if localMember.Status != receivedMember.Status {
			LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed to %s (from %s) during merge", receivedMember.ID(), receivedMember.Status, localMember.Status)
		}
		return receivedMember
	}
	if localMember.Status == StatusDead || localMember.Status == StatusVoluntaryLeave {
		return localMember // Keep local final status
	}
	if receivedMember.Status == localMember.Status {
		switch Config.Protocol {
		case GossipProtocol:
			// For gossip without suspicion: only return if higher heartbeat
			if receivedMember.Heartbeat > localMember.Heartbeat {
				receivedMember.LastUpdated = time.Now()

				return receivedMember
			}
		case PingAckProtocol:
			if receivedMember.ID() == receiverId {
				receivedMember.LastUpdated = time.Now()
				return receivedMember
			}
		}
	} else {
		// Different status, same incarnation: Suspect > Alive
		if compareStatus(receivedMember.Status, localMember.Status) {
			LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed from %s to %s during merge", receivedMember.ID(), localMember.Status, receivedMember.Status)
			receivedMember.LastUpdated = time.Now()
			return receivedMember
		}
		return localMember
	}
	return localMember
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
