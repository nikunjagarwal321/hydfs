package main

import (
	"fmt"
	"math/rand"
	"time"
)

// RPC Service definitions
type DistributedSystemService struct {
	server *Server
}

// Request/Response types for RPC calls
type JoinRequest struct {
	Member Member `json:"member"`
}

type JoinResponse struct {
	Success        bool          `json:"success"`
	Message        string        `json:"message"`
	MembershipList []Member      `json:"membership_list"`
	Protocol       ProtocolType  `json:"protocol_type"`
	Suspicion      SuspicionType `json:"suspicion_type"`
	IntroducerID   string        `json:"introducer_id"`
}

type GossipRequest struct {
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type GossipResponse struct {
	Success bool `json:"success"`
}

type PingRequest struct {
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type Ack struct { // CAN BE TAKEN AS ACK RESPONSE??
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type ProtocolSwitchRequest struct {
	SenderID  string        `json:"sender_address"`
	Protocol  ProtocolType  `json:"protocol"`
	Suspicion SuspicionType `json:"suspicion"`
}

type ProtocolSwitchResponse struct {
	Success bool `json:"success"`
}

// RPC Methods

// Join handles new member joining
func (ds *DistributedSystemService) Join(req *JoinRequest, resp *JoinResponse) error {
	if ds.server.IsIntroducer {
		req.Member.LastUpdated = time.Now()
		ds.server.Members.AddOrUpdate(req.Member)
		LogInfo(true, "MEMBER_JOIN: Introducer accepted new member: %s", req.Member.ID())
		ConsolePrintf("MEMBER_JOIN: Introducer accepted new member: %s\n", req.Member.ID())
		resp.Success = true
		resp.Message = "Successfully joined the cluster"
		resp.MembershipList = ds.server.Members.GetAll()
		resp.Protocol = Config.Protocol
		resp.Suspicion = Config.Suspicion
		resp.IntroducerID = ds.server.ID()
		return nil
	}
	resp.Success = false
	resp.Message = "Not an introducer"
	return nil
}

// Gossip handles gossip protocol messages
func (ds *DistributedSystemService) Gossip(req *GossipRequest, resp *GossipResponse) error {
	if globalServer != nil {
		mergeMembership(globalServer, req.MembershipList, req.SenderID)
		resp.Success = true
	} else {
		resp.Success = false
	}
	return nil
}

// Ping handles SWIM ping messages (for future SWIM implementation)
func (ds *DistributedSystemService) Ping(req *PingRequest, resp *Ack) error {
	LogInfo(false, "PingAck: Received PING from %s", req.SenderID)

	// Merge received membership list
	if globalServer != nil {
		mergeMembership(globalServer, req.MembershipList, req.SenderID)
	}

	// Will treat this as ACK for now. TODO: Introduce timeout or send a separate message response
	resp.SenderID = ds.server.ID()
	resp.MembershipList = ds.server.Members.GetAll()

	return nil
}

// ProtocolSwitch handles protocol change broadcast messages
func (ds *DistributedSystemService) ProtocolSwitch(req *ProtocolSwitchRequest, resp *ProtocolSwitchResponse) error {
	LogInfo(true, "Received protocol switch request from %s: Protocol=%s, Suspicion=%s",
		req.SenderID, req.Protocol, req.Suspicion)

	// Apply the protocol switch
	SwitchProtocol(req.Protocol, req.Suspicion)

	resp.Success = true
	LogInfo(true, "Successfully switched protocol to: %s, %s", req.Protocol, req.Suspicion)
	return nil
}

// RPC Client helper functions

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
	// Get membership list from global server
	req := &PingRequest{
		SenderID:       senderId,
		MembershipList: membersList,
	}
	var resp Ack
	// TODO: Add a timeout for which you want to wait incase you dont want to get a response.
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
