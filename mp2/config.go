package main

import (
	"strconv"
	"time"
)

type ProtocolType string

type SuspicionType string

var heartbeatInterval = 100 * time.Millisecond
var suspicionCheckTimeout = 1 * time.Second
var suspicionTimeout = 2 * time.Second
var deadTimeout = 2 * time.Second
var cleanUpTimeout = 2 * time.Second
var gossipOrSwimPingInterval = 100 * time.Millisecond
var PingFanout = 1
var GossipFanout = 1
var InitialProtocol = PingAckProtocol
var InitialSuspicion = NoSuspect
var InitialMessageDropRate = 0.0
var AckTimeout = 1 * time.Second

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
	"vm1":  "127.0.0.1:5000",
	"vm2":  "127.0.0.1:5001",
	"vm3":  "127.0.0.1:5002",
	"vm4":  "127.0.0.1:5003",
	"vm5":  "127.0.0.1:5004",
	"vm6":  "127.0.0.1:5005",
	"vm7":  "127.0.0.1:5006",
	"vm8":  "127.0.0.1:5007",
	"vm9":  "127.0.0.1:5008",
	"vm10": "127.0.0.1:5009",
}

var Config = struct {
	IntroducerAddr    string
	Protocol          ProtocolType
	Fanout            int
	AllNodes          []string
	Suspicion         SuspicionType
	MessageDropRate   float64 // Percentage of messages to drop (0.0 to 1.0)
	HashBits          int     // Number of bits for hash function
	ReplicationFactor int     // Number of replicas for HyDFS files
}{
	IntroducerAddr:    "127.0.0.1:5000",
	Protocol:          InitialProtocol,
	Fanout:            PingFanout,             // Number of random nodes to gossip to
	AllNodes:          []string{},             // Will be populated dynamically
	Suspicion:         InitialSuspicion,       // Enable suspicion mechanism by default
	MessageDropRate:   InitialMessageDropRate, // No message drop by default
	HashBits:          8,                      // Default to 8 bits for hash function
	ReplicationFactor: 3,                      // Default to 3 replicas (including primary)
	//TODO : Check if any other config is needed for HyDFS
}

// SwitchProtocol and ToggleSuspicion allows dynamic protocol and suspicion type switching at runtime
func SwitchProtocol(newProtocol ProtocolType, newSuspicionType SuspicionType) {
	Config.Protocol = newProtocol
	Config.Suspicion = newSuspicionType
	if newProtocol == PingAckProtocol {
		Config.Fanout = PingFanout
	} else {
		Config.Fanout = GossipFanout
	}
	LogInfo(true, "New Configs: {%s, %s}", newProtocol, newSuspicionType)
}

// SetMessageDropRate sets the message drop rate for testing network failures
func SetMessageDropRate(percentage string) {
	dropRate, err := strconv.ParseFloat(percentage, 64)
	if err != nil {
		ConsolePrintf("Invalid percentage: %s\n", percentage)
		return
	}

	if dropRate < 0.0 {
		dropRate = 0.0
	}
	if dropRate > 1.0 {
		dropRate = 1.0
	}
	Config.MessageDropRate = dropRate
	ConsolePrintf("Message drop rate set to: %.2f%%\n", dropRate*100)
}
