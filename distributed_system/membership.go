package main

import (
	"fmt"
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

func (ml *MembershipList) MarkSuspectIfNeeded(suspicionTimeout, deadTimeout time.Duration) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	changed := false
	now := time.Now()

	for id, m := range ml.nodes {
		elapsed := now.Sub(m.LastUpdated)

		if m.Status == StatusAlive && elapsed > suspicionTimeout {
			m.MarkSuspect()
			ml.nodes[id] = m
			changed = true
		} else if m.Status == StatusSuspect && elapsed > deadTimeout {
			m.MarkDead()
			ml.nodes[id] = m
			changed = true
		}
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

func (ml *MembershipList) Print() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	fmt.Println("---- Membership List ----")
	for id, m := range ml.nodes {
		fmt.Printf("Node: %s, Status: %s, HB: %d\n", id, m.Status, m.Heartbeat)
	}
	fmt.Println("-------------------------")
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
