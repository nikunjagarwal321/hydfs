package main

import (
	"fmt"
	"strings"
	"time"
)

type Status string

const (
	StatusAlive          Status = "alive"
	StatusSuspect        Status = "suspect"
	StatusDead           Status = "dead"
	StatusVoluntaryLeave Status = "voluntary_leave"
)

// TO GET A MEMBER, ALWAYS USE ID
type Member struct {
	Address               string    `json:"address"`
	NodeCreationTimestamp time.Time `json:"node_creation_timestamp"`
	Status                Status    `json:"status"`
	Heartbeat             uint64    `json:"heartbeat"`
	Incarnation           uint64    `json:"incarnation"`
	LastUpdated           time.Time `json:"last_updated"`
}

func (m *Member) ID() string {
	return fmt.Sprintf("%s-%d", m.Address, m.NodeCreationTimestamp.UnixNano())
}

func GetAddressFromID(memberID string) string {
	lastDash := strings.LastIndex(memberID, "-")
	if lastDash == -1 {
		return ""
	}
	return memberID[:lastDash]
}

func (m *Member) MarkSuspect() {
	m.Status = StatusSuspect
	m.LastUpdated = time.Now()
}

func (m *Member) MarkDead() {
	m.Status = StatusDead
	m.LastUpdated = time.Now()
}

func (m *Member) MarkVoluntaryLeave() {
	m.Status = StatusVoluntaryLeave
	m.LastUpdated = time.Now()
}

func (m *Member) IsAlive() bool {
	return m.Status == StatusAlive
}
