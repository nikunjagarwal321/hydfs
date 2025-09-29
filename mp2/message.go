package main

type MessageType string

const (
	NewJoinee MessageType = "NEW_JOINEE"
	Heartbeat MessageType = "HEARTBEAT"
	GossipMsg MessageType = "GOSSIP"
)

type Message struct {
	Type   MessageType `json:"type"`
	Member Member      `json:"member"`
	Data   []byte      `json:"data,omitempty"` // For gossip payload
}
