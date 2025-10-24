package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// makeRPCCall is a generic RPC call function over UDP with bandwidth tracking
func makeRPCCall(address string, method string, params interface{}, result interface{}, server *Server) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	// Set timeout for the connection
	conn.SetReadDeadline(time.Now().Add(AckTimeout))

	// Create RPC message
	rpcMsg := RPCMessage{
		Method: method,
		Params: params,
		ID:     uint64(time.Now().UnixNano()),
	}

	// Send request
	data, err := json.Marshal(rpcMsg)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	bytesWritten, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	// Track bytes sent
	if server != nil && server.BandwidthStats != nil {
		server.BandwidthStats.AddSent(uint64(bytesWritten))
	}

	// Read response
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	// Track bytes received
	if server != nil && server.BandwidthStats != nil {
		server.BandwidthStats.AddReceived(uint64(n))
	}

	var rpcResp RPCResponse
	err = json.Unmarshal(buffer[:n], &rpcResp)
	if err != nil {
		return fmt.Errorf("unmarshal response error: %w", err)
	}

	if rpcResp.Error != "" {
		return fmt.Errorf("RPC error: %s", rpcResp.Error)
	}

	// Convert result back to expected type
	resultData, err := json.Marshal(rpcResp.Result)
	if err != nil {
		return fmt.Errorf("marshal result error: %w", err)
	}

	err = json.Unmarshal(resultData, result)
	if err != nil {
		return fmt.Errorf("unmarshal result error: %w", err)
	}

	return nil
}

// CallJoin makes an RPC call to join the cluster
func CallJoin(address string, member Member, server *Server) (*JoinResponse, error) {
	req := &JoinRequest{Member: member}
	var resp JoinResponse
	err := makeRPCCall(address, "Join", req, &resp, server)
	return &resp, err
}

// CallGossip makes an RPC call to send gossip
func CallGossip(address string, senderId string, membersList []Member, server *Server) (*GossipResponse, error) {
	req := &GossipRequest{
		SenderID:       senderId,
		MembershipList: membersList,
	}
	var resp GossipResponse
	err := makeRPCCall(address, "Gossip", req, &resp, server)
	return &resp, err
}

// CallPing makes an RPC call to ping a node
// In Ping-Ack, we will also merge the membership list received from ACK
func CallPing(address string, senderId string, membersList []Member, server *Server) (*Ack, error) {
	req := &PingRequest{
		SenderID:       senderId,
		MembershipList: membersList,
	}
	var resp Ack
	LogInfo(false, "PingAck: Sending Ping to %s", address)
	err := makeRPCCall(address, "Ping", req, &resp, server)

	// Simulate ACK drop at the caller side
	if err == nil && Config.MessageDropRate > 0.0 {
		if rand.Float64() < Config.MessageDropRate {
			LogInfo(true, "DROPPED ACK from %s (drop rate: %.2f%%)", address, Config.MessageDropRate*100)
			return nil, fmt.Errorf("ACK dropped")
		}
	}

	return &resp, err
}

// CallProtocolSwitch makes an RPC call to broadcast protocol switch
func CallProtocolSwitch(address string, senderID string, protocol ProtocolType, suspicion SuspicionType, server *Server) (*ProtocolSwitchResponse, error) {
	req := &ProtocolSwitchRequest{
		SenderID:  senderID,
		Protocol:  protocol,
		Suspicion: suspicion,
	}
	var resp ProtocolSwitchResponse
	err := makeRPCCall(address, "ProtocolSwitch", req, &resp, server)
	return &resp, err
}
