package main

import (
	"encoding/json"
	"fmt"
	"net"
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
	Success        bool     `json:"success"`
	Message        string   `json:"message"`
	MembershipList []Member `json:"membership_list"`
}

type GossipRequest struct {
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type GossipResponse struct {
	Success bool `json:"success"`
}

type PingRequest struct {
	SenderID       string            `json:"sender_address"`
	TargetAddress  string            `json:"target_address"`
	Sequence       uint64            `json:"sequence"`
	MembershipList map[string]Member `json:"membership_list"`
}

type Ack struct { // CAN BE TAKEN AS ACK RESPONSE??
	Success        bool              `json:"success"`
	SenderID       string            `json:"sender_address"`
	Sequence       uint64            `json:"sequence"`
	MembershipList map[string]Member `json:"membership_list"`
}

// RPC Methods

// Join handles new member joining
func (ds *DistributedSystemService) Join(req *JoinRequest, resp *JoinResponse) error {
	if ds.server.IsIntroducer {
		req.Member.LastUpdated = time.Now()
		ds.server.Members.AddOrUpdate(req.Member)
		resp.Success = true
		resp.Message = "Successfully joined the cluster"
		// TODO: Fix this to respond with members
		resp.MembershipList = ds.server.Members.GetAll()
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
	fmt.Printf("SWIM: Received PING from %s (sequence: %d)\n", req.SenderID, req.Sequence)

	// Merge received membership list
	if globalServer != nil {
		members := convertMapToSlice(req.MembershipList)
		mergeMembership(globalServer, members)
	}

	resp.Success = true
	resp.SenderID = ds.server.ID()
	resp.Sequence = req.Sequence
	resp.MembershipList = ds.server.Members.Snapshot()

	fmt.Printf("SWIM: Sending ACK to %s (sequence: %d)\n", req.SenderID, req.Sequence)
	return nil
}

// RPC Message wrapper
type RPCMessage struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	ID     uint64      `json:"id"`
}

type RPCResponse struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error,omitempty"`
	ID     uint64      `json:"id"`
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

// CallPing makes an RPC call to ping a node (for SWIM)
func CallPing(address string, senderAddr string, targetAddr string, sequence uint64) (*Ack, error) {
	// Get membership list from global server
	var membershipList map[string]Member
	if globalServer != nil {
		membershipList = globalServer.Members.Snapshot()
	}

	req := &PingRequest{
		SenderID:       senderAddr,
		TargetAddress:  targetAddr,
		Sequence:       sequence,
		MembershipList: membershipList,
	}
	var resp Ack
	err := makeRPCCall(address, "Ping", req, &resp)

	if err == nil {
		fmt.Printf("SWIM: Received ACK from %s (sequence: %d)\n", address, sequence)
	}

	// Merge received membership list
	if err == nil && globalServer != nil {
		members := convertMapToSlice(resp.MembershipList)
		mergeMembership(globalServer, members)
	}

	return &resp, err
}

// convertMapToSlice converts membership map to slice
func convertMapToSlice(membershipMap map[string]Member) []Member {
	var members []Member
	for _, member := range membershipMap {
		members = append(members, member)
	}
	return members
}

// makeRPCCall is a generic RPC call function over UDP
func makeRPCCall(address string, method string, params interface{}, result interface{}) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	// Set timeout for the connection
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

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

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	// Read response
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
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

// StartRPCServer starts the RPC server with UDP transport
// ADD ALL LOGIC TO BE RUN IN PARALLEL HERE
func (s *Server) StartRPCServer() error {
	addr, err := net.ResolveUDPAddr("udp", s.Addr)
	if err != nil {
		return fmt.Errorf("resolve address error: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}
	defer conn.Close()

	fmt.Printf("RPC Server listening on %s, Introducer: %v\n", s.Addr, s.IsIntroducer)

	// Set global server for gossip handling
	globalServer = s

	// Join introducer if not introducer
	if !s.IsIntroducer {
		err = s.notifyIntroducer()
		if err != nil {
			fmt.Printf("Failed to start the node: %v\n", err)
			return err
		}
	}

	// Start background processes
	go s.listenForMessages(conn)
	go s.sendTimelyMessagesAsPerProtocol(gossipOrSwimPingInterval)
	go s.increaseHeartbeat(heartbeatInterval)
	go s.backgroundCheckerRPC(suspicionCheckTimeout, suspicionTimeout, deadTimeout)
	select {}
}

func (s *Server) increaseHeartbeat(heartbeatInterval time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Increment the server's heartbeat counter
		s.HeartbeatCounter++

		// Create updated member info for self
		selfMember := Member{
			Address:               s.Addr,
			NodeCreationTimestamp: s.NodeCreationTimestamp,
			Status:                StatusAlive,
			Heartbeat:             s.HeartbeatCounter,
			Incarnation:           s.IncarnationNumber,
			LastUpdated:           time.Now(),
		}

		// Update self in the membership list
		s.Members.AddOrUpdate(selfMember)

		// fmt.Printf("[%s] Heartbeat increased to %d\n", s.ID(), s.HeartbeatCounter)
	}
}

func (s *Server) notifyIntroducer() error {
	member := Member{
		Address:               s.Addr,
		NodeCreationTimestamp: s.NodeCreationTimestamp,
		Status:                StatusAlive,
		Heartbeat:             s.HeartbeatCounter,
		Incarnation:           s.IncarnationNumber,
		LastUpdated:           time.Now(),
	}

	resp, err := CallJoin(s.IntroducerAddr, member)
	if err != nil {
		fmt.Printf("Failed to join cluster: %v\n", err)
		return err
	} else {
		fmt.Printf("Join response: %+v\n", resp)
		mergeMembership(s, resp.MembershipList)
		return nil
	}
}

func (s *Server) listenForMessages(conn *net.UDPConn) {
	// Handle incoming RPC calls
	service := &DistributedSystemService{server: s}
	buffer := make([]byte, 4096)

	for {
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("Read error: %v\n", err)
			continue
		}

		go s.handleRPCRequest(conn, clientAddr, buffer[:n], service)
	}
}

// handleRPCRequest processes incoming RPC requests
// ADD DIFFERENT TYPES OF REQUEST/RESPONSE MESSAGES HERE. FOR SWIM AS WELL --> PING, ACK, SUSPECT, SUSPECT-RESP
func (s *Server) handleRPCRequest(conn *net.UDPConn, clientAddr *net.UDPAddr, data []byte, service *DistributedSystemService) {
	var rpcMsg RPCMessage
	err := json.Unmarshal(data, &rpcMsg)
	if err != nil {
		s.sendRPCError(conn, clientAddr, 0, fmt.Sprintf("unmarshal error: %v", err))
		return
	}

	var result interface{}
	var rpcErr error

	// Route to appropriate method
	switch rpcMsg.Method {
	case "Join":
		var req JoinRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp JoinResponse
		rpcErr = service.Join(&req, &resp)
		result = resp

	case "Gossip":
		var req GossipRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp GossipResponse
		rpcErr = service.Gossip(&req, &resp)
		result = resp

	case "Ping":
		var req PingRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp Ack
		rpcErr = service.Ping(&req, &resp)
		result = resp

	default:
		s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("unknown method: %s", rpcMsg.Method))
		return
	}

	if rpcErr != nil {
		s.sendRPCError(conn, clientAddr, rpcMsg.ID, rpcErr.Error())
		return
	}

	s.sendRPCResponse(conn, clientAddr, rpcMsg.ID, result)
}

// convertParams converts interface{} params to specific struct type
func (s *Server) convertParams(params interface{}, target interface{}) error {
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// sendRPCResponse sends a successful RPC response
func (s *Server) sendRPCResponse(conn *net.UDPConn, clientAddr *net.UDPAddr, id uint64, result interface{}) {
	resp := RPCResponse{
		Result: result,
		ID:     id,
	}
	data, _ := json.Marshal(resp)
	conn.WriteToUDP(data, clientAddr)
}

// sendRPCError sends an RPC error response
func (s *Server) sendRPCError(conn *net.UDPConn, clientAddr *net.UDPAddr, id uint64, errMsg string) {
	resp := RPCResponse{
		Error: errMsg,
		ID:    id,
	}
	data, _ := json.Marshal(resp)
	conn.WriteToUDP(data, clientAddr)
}
