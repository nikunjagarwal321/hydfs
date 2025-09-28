package main

import (
	"sync"
	"time"
)

type MembershipList struct {
	mu    sync.Mutex
	nodes map[string]Member
}

func NewMembershipList() *MembershipList {
	return &MembershipList{nodes: make(map[string]Member)}
}

func (ml *MembershipList) AddOrUpdate(member Member) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.nodes[member.ID()] = member
}

func (ml *MembershipList) Remove(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	delete(ml.nodes, nodeID)
}

func (ml *MembershipList) MarkSuspectIfNeeded(suspicionTimeout, deadTimeout, cleanUpTimeout time.Duration) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	changed := false
	now := time.Now()

	// Collect changes to avoid modifying map during iteration
	var toUpdate []Member
	var toRemove []string

	for id, m := range ml.nodes {

		// Skip self check for suspicion if protocol is SWIM
		if Config.Protocol == PingAckProtocol {
			if m.Address == globalServer.Addr {
				continue
			}
		}

		elapsed := now.Sub(m.LastUpdated)

		if m.Status == StatusAlive && elapsed > suspicionTimeout {
			switch Config.Suspicion {
			case Suspect:
				m.MarkSuspect()
				toUpdate = append(toUpdate, m)
				LogInfo(true, "SUSPECT_DETECTED: Marked member as SUSPECT: %s", m.ID())
				ConsolePrintf("SUSPECT_DETECTED: Marked member as SUSPECT: %s\n", m.ID())
				changed = true
			case NoSuspect:
				// In no-suspicion mode, only mark as dead if it crosses dead timeout
				if elapsed > deadTimeout {
					m.MarkDead()
					toUpdate = append(toUpdate, m)
					LogInfo(true, "FAILURE_DETECTED: Marked member as DEAD: %s", m.ID())
					ConsolePrintf("FAILURE_DETECTED: Marked member as DEAD: %s\n", m.ID())
					changed = true
				}
			}
		} else if m.Status == StatusSuspect && elapsed > deadTimeout {
			m.MarkDead()
			toUpdate = append(toUpdate, m)
			LogInfo(true, "FAILURE_CONFIRMED: Marked suspected member as DEAD: %s", m.ID())
			ConsolePrintf("FAILURE_CONFIRMED: Marked suspected member as DEAD: %s\n", m.ID())
			changed = true
		} else if m.Status == StatusDead && elapsed > cleanUpTimeout {
			toRemove = append(toRemove, id)
			LogInfo(true, "MEMBER_CLEANUP: Removed dead member from list: %s", id)
			ConsolePrintf("MEMBER_CLEANUP: Removed dead member from list: %s\n", id)
			changed = true
		}
	}

	// Apply updates after iteration
	for _, member := range toUpdate {
		ml.nodes[member.ID()] = member
	}

	// Apply removals after iteration
	for _, id := range toRemove {
		delete(ml.nodes, id)
	}

	return changed
}

func (ml *MembershipList) Snapshot() map[string]Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	cp := make(map[string]Member)
	for k, v := range ml.nodes {
		cp[k] = v
	}
	// returns Nodes Key - ID, Nodes Value - Member
	return cp
}

func (ml *MembershipList) GetAll() []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	members := make([]Member, 0, len(ml.nodes))
	for _, m := range ml.nodes {
		members = append(members, m)
	}
	return members
}

func (ml *MembershipList) Print(printOnConsole bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if printOnConsole {
		ConsolePrintln("---- Membership List ----")
	}
	for _, member := range ml.nodes {
		if Config.Protocol == GossipProtocol {
			if printOnConsole {
				ConsolePrintf("Member: %s | Status:  %s| Heartbeat: %d| Incarnation: %d| LastUpdated: %s\n", member.ID(), member.Status, member.Heartbeat, member.Incarnation, member.LastUpdated.Format(time.RFC3339))
			}
			LogInfo(true, "Member: %s | Status:  %s| Heartbeat: %d| Incarnation: %d| LastUpdated: %s\n", member.ID(), member.Status, member.Heartbeat, member.Incarnation, member.LastUpdated.Format(time.RFC3339))
		}
		if Config.Protocol == PingAckProtocol {
			if printOnConsole {
				ConsolePrintf("Member: %s | Status:  %s| Incarnation: %d\n", member.ID(), member.Status, member.Incarnation)
			}
			LogInfo(true, "Member: %s | Status:  %s| Incarnation: %d\n", member.ID(), member.Status, member.Incarnation)
		}
	}
	if printOnConsole {
		ConsolePrintln("-------------------------")
	}
}

func (ml *MembershipList) PrintSuspectedNodes() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	ConsolePrintln("---- Membership List ----")
	for _, member := range ml.nodes {
		if member.Status == StatusSuspect {
			if Config.Protocol == GossipProtocol {
				ConsolePrintf("Suspected Member: %s | Status:  %s| Heartbeat: %d| Incarnation: %d| LastUpdated: %s\n", member.ID(), member.Status, member.Heartbeat, member.Incarnation, member.LastUpdated.Format(time.RFC3339))
				LogInfo(true, "Suspected Member: %s | Status:  %s| Heartbeat: %d| Incarnation: %d| LastUpdated: %s\n", member.ID(), member.Status, member.Heartbeat, member.Incarnation, member.LastUpdated.Format(time.RFC3339))
			}
			if Config.Protocol == PingAckProtocol {
				ConsolePrintf("Suspected Member: %s | Status:  %s| Incarnation: %d\n", member.ID(), member.Status, member.Incarnation)
				LogInfo(true, "Suspected Member: %s | Status:  %s| Incarnation: %d\n", member.ID(), member.Status, member.Incarnation)
			}
		}
	}
	ConsolePrintln("-------------------------")

}

// GetSuspectedNodes returns all suspected nodes from the membership list
func (ml *MembershipList) GetSuspectedNodes() []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	var suspectedNodes []Member
	for _, member := range ml.nodes {
		if member.Status == StatusSuspect {
			suspectedNodes = append(suspectedNodes, member)
		}
	}
	return suspectedNodes
}
