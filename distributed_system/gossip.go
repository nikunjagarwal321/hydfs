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
	SuspectedNodes []string `json:"suspected_nodes"`
}

// Global server instance (will be set in main)
var globalServer *Server

// Pick b random nodes excluding self
// TODO: Can implement round robin in future
func selectRandomNodes(self string, membershipSnapshot map[string]Member, b int) []string {
	var candidates []string
	for nodeId, member := range membershipSnapshot {
		if nodeId != self && member.Status != StatusDead {
			candidates = append(candidates, member.Address)
		}
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	if b > len(candidates) {
		b = len(candidates)
	}
	return candidates[:b]
}

// Send gossip to random nodes
func (s *Server) gossipSend() {

	snapshot := s.Members.Snapshot()
	nodes := selectRandomNodes(s.ID(), snapshot, Config.GossipFanout)

	if len(nodes) == 0 {
		return // No nodes to gossip to
	}

	suspected := []Member{}
	membersList := []Member{}
	for _, m := range snapshot {
		membersList = append(membersList, m)
		if m.Status == StatusSuspect {
			suspected = append(suspected, m)
		}
	}

	for _, node := range nodes {
		go func(nodeAddr string) {
			resp, err := CallGossip(nodeAddr, s.ID(), membersList, suspected)
			if err != nil {
				fmt.Printf("Failed to send gossip to %s: %v\n", nodeAddr, err)
			} else {
				fmt.Printf("Sent gossip to %s, success: %v\n", nodeAddr, resp.Success)
			}
		}(node)
	}
}

// mergeMembership merges the received membership list with local membership
// TODO: Verify the logic for Gossip and SWIM and decouple both the merge logic
func mergeMembership(server *Server, receivedMembers []Member, suspectedNodes []Member) {
	localSnapshot := server.Members.Snapshot()

	for _, receivedMember := range receivedMembers {
		localMember, exists := localSnapshot[receivedMember.ID()]

		if !exists {
			// New member, add it
			receivedMember.LastUpdated = time.Now()
			server.Members.AddOrUpdate(receivedMember)
			fmt.Printf("Added new member: %s\n", receivedMember.ID())
			continue
		}

		// Member exists, check which version is more recent
		shouldUpdate := false

		// Rule 1: Higher incarnation number wins
		if receivedMember.Incarnation > localMember.Incarnation {
			shouldUpdate = true
		} else if receivedMember.Incarnation == localMember.Incarnation {
			// Rule 2: If incarnations are equal, higher heartbeat wins
			if receivedMember.Heartbeat > localMember.Heartbeat {
				shouldUpdate = true
			} else if receivedMember.Heartbeat == localMember.Heartbeat {
				// Rule 3: If heartbeats are equal, prefer alive over suspect, suspect over dead
				localPriority := getStatusPriority(localMember.Status)
				receivedPriority := getStatusPriority(receivedMember.Status)
				if receivedPriority > localPriority {
					shouldUpdate = true
				}
			}
		}

		if shouldUpdate {
			receivedMember.LastUpdated = time.Now()
			server.Members.AddOrUpdate(receivedMember)
			fmt.Printf("Updated member: %s (Inc: %d->%d, HB: %d->%d, Status: %s->%s)\n",
				receivedMember.ID(),
				localMember.Incarnation, receivedMember.Incarnation,
				localMember.Heartbeat, receivedMember.Heartbeat,
				localMember.Status, receivedMember.Status)
		}
	}

	// Handle suspected nodes separately if needed
	for _, suspectedAddr := range suspectedNodes {
		if localMember, exists := localSnapshot[suspectedAddr.ID()]; exists {
			if localMember.Status == StatusAlive {
				// Only mark as suspect if currently alive
				localMember.Status = StatusSuspect
				localMember.LastUpdated = time.Now()
				server.Members.AddOrUpdate(localMember)
				fmt.Printf("Marked member as suspected: %s\n", suspectedAddr)
			}
		}
	}
}

// getStatusPriority returns priority for status comparison (higher is better)
func getStatusPriority(status Status) int {
	switch status {
	case StatusAlive:
		return 3
	case StatusSuspect:
		return 2
	case StatusDead:
		return 1
	default:
		return 0
	}
}
