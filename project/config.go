package main

type NodeConfig struct {
	ID    string   // Node name
	Port  string   // Port for RPC server
	Peers []string // Addresses of peers in "ip:port" format
}

var nodeMap = map[string]NodeConfig{
	"NodeA": {"NodeA", "8001", []string{"localhost:8002", "localhost:8003"}},
	"NodeB": {"NodeB", "8002", []string{"localhost:8001", "localhost:8003"}},
	"NodeC": {"NodeC", "8003", []string{"localhost:8001", "localhost:8002"}},
}
