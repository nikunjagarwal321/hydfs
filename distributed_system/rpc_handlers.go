package main

import (
	"fmt"
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
		resp.Success = true
		resp.Message = "Successfully joined the cluster"
		resp.MembershipList = ds.server.Members.GetAll()
		resp.Protocol = Config.Protocol
		resp.Suspicion = Config.Suspicion
		return nil
	}
	resp.Success = false
	resp.Message = "Not an introducer"
	return nil
}

// Gossip handles gossip protocol messages
func (ds *DistributedSystemService) Gossip(req *GossipRequest, resp *GossipResponse) error {
	if globalServer != nil {
		mergeMembership(globalServer, req.MembershipList)
		resp.Success = true
	} else {
		resp.Success = false
	}
	return nil
}

// Ping handles SWIM ping messages (for future SWIM implementation)
func (ds *DistributedSystemService) Ping(req *PingRequest, resp *Ack) error {
	fmt.Printf("PingAck: Received PING from %s \n", req.SenderID)

	// Merge received membership list
	if globalServer != nil {
		mergeMembership(globalServer, req.MembershipList)
	}

	// Will treat this as ACK for now. TODO: Introduce timeout or send a separate message response
	resp.SenderID = ds.server.ID()
	resp.MembershipList = ds.server.Members.GetAll()

	return nil
}

// ProtocolSwitch handles protocol change broadcast messages
func (ds *DistributedSystemService) ProtocolSwitch(req *ProtocolSwitchRequest, resp *ProtocolSwitchResponse) error {
	fmt.Printf("Received protocol switch request from %s: Protocol=%s, Suspicion=%s\n",
		req.SenderID, req.Protocol, req.Suspicion)

	// Apply the protocol switch
	SwitchProtocol(req.Protocol, req.Suspicion)

	resp.Success = true
	fmt.Printf("Successfully switched protocol to: %s, %s\n", req.Protocol, req.Suspicion)
	return nil
}

// RPC Client helper functions

// CallJoin makes an RPC call to join the cluster
func CallJoin(address string, member Member) (*JoinResponse, error) {
	req := &JoinRequest{Member: member}
	var resp JoinResponse
	err := makeRPCCall(address, "Join", req, &resp)
	return &resp, err
}

// CallGossip makes an RPC call to send gossip
func CallGossip(address string, senderId string, membersList []Member) (*GossipResponse, error) {
	req := &GossipRequest{
		SenderID:       senderId,
		MembershipList: membersList,
	}
	var resp GossipResponse
	err := makeRPCCall(address, "Gossip", req, &resp)
	return &resp, err
}

// CallPing makes an RPC call to ping a node
// In Ping-Ack, we will also merge the membership list received from ACK
func CallPing(address string, senderId string, membersList []Member) (*Ack, error) {
	// Get membership list from global server
	req := &PingRequest{
		SenderID:       senderId,
		MembershipList: membersList,
	}
	var resp Ack
	// TODO: Add a timeout for which you want to wait incase you dont want to get a response.
	err := makeRPCCall(address, "Ping", req, &resp)

	if err == nil {
		fmt.Printf("PingAck: Received ACK from %s\n", address)
	}

	// Merge received membership list
	if err == nil && globalServer != nil {
		mergeMembership(globalServer, resp.MembershipList)
	}

	return &resp, err
}

// CallProtocolSwitch makes an RPC call to broadcast protocol switch
func CallProtocolSwitch(address string, senderID string, protocol ProtocolType, suspicion SuspicionType) (*ProtocolSwitchResponse, error) {
	req := &ProtocolSwitchRequest{
		SenderID:  senderID,
		Protocol:  protocol,
		Suspicion: suspicion,
	}
	var resp ProtocolSwitchResponse
	err := makeRPCCall(address, "ProtocolSwitch", req, &resp)
	return &resp, err
}
