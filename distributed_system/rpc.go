package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

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
	cmdChan := make(chan string)

	go s.listenForMessages(conn)
	go s.sendTimelyMessagesAsPerProtocol(gossipOrSwimPingInterval)
	go s.increaseHeartbeat(heartbeatInterval)
	go s.backgroundCheckerRPC(suspicionCheckTimeout, suspicionTimeout, deadTimeout)
	go s.startCLI(cmdChan)

	// main loop processes commands
	for cmd := range cmdChan {
		s.handleCommand(cmd)
	}
	return nil
}

func (s *Server) startCLI(cmdChan chan<- string) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmdChan <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}

func (s *Server) handleCommand(cmd string) {
	// Split command into parts at the beginning
	parts := strings.Fields(strings.TrimSpace(cmd))
	if len(parts) == 0 {
		return
	}

	command := parts[0]

	switch command {
	case "list_mem":
		s.Members.Print()
	case "list_self":
		fmt.Printf("Self ID: %s\n", s.ID())
	case "leave":
		s.leaveGroup()
	case "display_suspects":
		s.Members.PrintSuspectedNodes()
	case "display_protocol":
		printCurrentProtocol()
	case "switch":
		if len(parts) != 3 {
			fmt.Printf("Invalid switch command format. Expected: switch <protocol> <suspicion>\n")
			return
		}
		s.handleSwitch(parts[1], parts[2])
	default:
		fmt.Println("Unknown command:", command)
	}
}

func (s *Server) leaveGroup() {
	fmt.Printf("Initiating graceful leave from the group...\n")

	// Get current membership snapshot
	snapshot := s.Members.Snapshot()
	selfMember, exists := snapshot[s.ID()]

	if !exists {
		fmt.Printf("Error: Self not found in membership list\n")
		return
	}

	// Mark self as voluntarily leaving
	selfMember.MarkVoluntaryLeave()
	selfMember.Incarnation = s.IncarnationNumber // Use current incarnation
	s.Members.AddOrUpdate(selfMember)

	fmt.Printf("Marked self as voluntarily leaving: %s\n", s.ID())
}

func (s *Server) handleSwitch(protocolStr, suspicionStr string) {
	protocolStr = strings.ToLower(protocolStr)
	suspicionStr = strings.ToLower(suspicionStr)

	protocol, ok := ProtocolMap[protocolStr]
	if !ok {
		fmt.Printf("Invalid protocol '%s'\n", protocolStr)
		return
	}

	suspicion, ok := SuspicionMap[suspicionStr]
	if !ok {
		fmt.Printf("Invalid suspicion type '%s'\n", suspicionStr)
		return
	}

	SwitchProtocol(protocol, suspicion)

	// Broadcast protocol switch to all other nodes in the membership list
	snapshot := s.Members.Snapshot()

	for _, member := range snapshot {
		// Skip self
		if member.Address != s.Addr {
			go func(nodeAddr string) {
				resp, err := CallProtocolSwitch(nodeAddr, s.ID(), protocol, suspicion)
				if err != nil {
					fmt.Printf("Failed to send protocol switch to %s: %v\n", nodeAddr, err)
				} else {
					fmt.Printf("Sent protocol switch to %s, success: %v\n", nodeAddr, resp.Success)
				}
			}(member.Address)
		}
	}

	fmt.Printf("Protocol switch broadcast completed\n")
}

func printCurrentProtocol() {
	fmt.Printf("Current Protocol: %s | Suspicion : %s\n", Config.Protocol, Config.Suspicion)
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

	case "ProtocolSwitch":
		var req ProtocolSwitchRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp ProtocolSwitchResponse
		rpcErr = service.ProtocolSwitch(&req, &resp)
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
