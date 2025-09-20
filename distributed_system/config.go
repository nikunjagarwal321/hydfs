package main

import (
	"fmt"
	"time"
)

type ProtocolType string

var heartbeatInterval = 1 * time.Second
var suspicionCheckTimeout = 5 * time.Second
var suspicionTimeout = 10 * time.Second
var deadTimeout = 20 * time.Second
var gossipOrSwimPingInterval = 10 * time.Second

const (
	GossipProtocol ProtocolType = "gossip"
	SwimProtocol   ProtocolType = "swim"
)

var NodeMap = map[string]string{
	"vm1": "127.0.0.1:5000",
	"vm2": "127.0.0.1:5001",
	"vm3": "127.0.0.1:5002",
}

var Config = struct {
	IntroducerAddr string
	Protocol       ProtocolType
	GossipFanout   int
	AllNodes       []string
}{
	IntroducerAddr: "127.0.0.1:5000",
	Protocol:       GossipProtocol,
	GossipFanout:   3,          // Number of random nodes to gossip to
	AllNodes:       []string{}, // Will be populated dynamically
}

// SwitchProtocol allows dynamic protocol switching at runtime
func SwitchProtocol(newProtocol ProtocolType) {
	Config.Protocol = newProtocol
	fmt.Printf("Protocol switched to: %s\n", newProtocol)
}
