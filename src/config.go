package main

type NodeConfig struct {
	ID    string   // Client name
	Port  string   // Port for RPC server
	Peers []string // Addresses of peers in "ip:port" format
}

// Map of client name → NodeConfig
var nodeMap = map[string]NodeConfig{
	"ClientA": {"ClientA", "8001", []string{"localhost:8002", "localhost:8003"}},
	"ClientB": {"ClientB", "8002", []string{"localhost:8001", "localhost:8003"}},
	"ClientC": {"ClientC", "8003", []string{"localhost:8001", "localhost:8002"}},
}
