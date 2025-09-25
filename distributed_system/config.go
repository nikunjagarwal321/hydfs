package main

import (
	"fmt"
	"time"
)

type ProtocolType string

type SuspicionType string

var heartbeatInterval = 1 * time.Second
var suspicionCheckTimeout = 5 * time.Second
var suspicionTimeout = 10 * time.Second
var deadTimeout = 20 * time.Second
var gossipOrSwimPingInterval = 10 * time.Second

// Use gossipOrSwimPingInterval and GossipFanout in conjunction

const (
	GossipProtocol  ProtocolType = "gossip"
	PingAckProtocol ProtocolType = "ping"
)

const (
	Suspect   SuspicionType = "suspect"
	NoSuspect SuspicionType = "nosuspect"
)

var ProtocolMap = map[string]ProtocolType{
	string(GossipProtocol):  GossipProtocol,
	string(PingAckProtocol): PingAckProtocol,
}

var SuspicionMap = map[string]SuspicionType{
	string(Suspect):   Suspect,
	string(NoSuspect): NoSuspect,
}

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
	Suspicion      SuspicionType
}{
	IntroducerAddr: "127.0.0.1:5000",
	Protocol:       GossipProtocol,
	GossipFanout:   3,          // Number of random nodes to gossip to
	AllNodes:       []string{}, // Will be populated dynamically
	Suspicion:      Suspect,    // Enable suspicion mechanism by default
}

// SwitchProtocol and ToggleSuspicion allows dynamic protocol and suspicion type switching at runtime
func SwitchProtocol(newProtocol ProtocolType, newSuspicionType SuspicionType) {
	Config.Protocol = newProtocol
	Config.Suspicion = newSuspicionType
	fmt.Printf("New Configs: {%s, %s}\n", newProtocol, newSuspicionType)
}
