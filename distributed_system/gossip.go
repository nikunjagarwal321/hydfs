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

// mergeMembership merges the received membership list with local membership
// TODO: Verify the logic for Gossip and SWIM and decouple both the merge logic
func mergeMembership(server *Server, receivedMembers []Member) {
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

		shouldUpdate := false

		// Rule 1: Dead always wins (but don't update if both are already dead)
		if receivedMember.Status == StatusDead && localMember.Status != StatusDead {
			shouldUpdate = true
		} else if localMember.Status == StatusDead {
			shouldUpdate = false
		} else {
			// Rule 2: Higher incarnation wins
			if receivedMember.Incarnation > localMember.Incarnation {
				shouldUpdate = true
			} else if receivedMember.Incarnation == localMember.Incarnation {
				// Special case: If we receive information about ourselves being marked as suspect
				// but we are actually alive, increment our incarnation number
				if receivedMember.Address == server.Addr &&
					receivedMember.Status == StatusSuspect &&
					localMember.Status == StatusAlive {
					// We are alive but others think we are suspect - increment incarnation
					server.IncarnationNumber++
					localMember.Incarnation = server.IncarnationNumber
					localMember.LastUpdated = time.Now()
					server.Members.AddOrUpdate(localMember)
					fmt.Printf("Incremented incarnation to %d - I'm alive but was marked as suspect\n",
						server.IncarnationNumber)
					continue
				}

				if Config.Protocol == SwimProtocol {
					// SWIM: ignore heartbeat, use status priority
					shouldUpdate = compareStatus(receivedMember.Status, localMember.Status)
				} else {
					// Gossip: higher heartbeat wins
					if receivedMember.Heartbeat > localMember.Heartbeat {
						shouldUpdate = true
					} else if receivedMember.Heartbeat == localMember.Heartbeat {
						// fall back to status priority
						shouldUpdate = compareStatus(receivedMember.Status, localMember.Status)
					}
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

}

// compareStatus returns true if received should replace local
func compareStatus(received, local Status) bool {
	return getStatusPriority(received) > getStatusPriority(local)
}

// getStatusPriority: Dead > Suspect > Alive
func getStatusPriority(status Status) int {
	switch status {
	case StatusDead:
		return 3
	case StatusSuspect:
		return 2
	case StatusAlive:
		return 1
	default:
		return 0
	}
}
