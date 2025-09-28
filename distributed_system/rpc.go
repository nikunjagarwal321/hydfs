package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
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

	ConsolePrintf("RPC Server listening on %s, Introducer: %v\n", s.Addr, s.IsIntroducer)

	// Set global server for gossip handling
	globalServer = s

	// Join introducer if not introducer
	if !s.IsIntroducer {
		err = s.notifyIntroducer()
		if err != nil {
			LogError(true, "Failed to start the node: %v", err)
			return err
		}
	}

	// Start background processes
	cmdChan := make(chan string)

	go s.listenForMessages(conn)
	go s.sendTimelyMessagesAsPerProtocol(gossipOrSwimPingInterval)
	go s.increaseHeartbeat(heartbeatInterval)
	go s.backgroundCheckerRPC(suspicionCheckTimeout, suspicionTimeout, deadTimeout, cleanUpTimeout)
	go s.monitorBandwidth()
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
		LogError(true, "Error reading input: %v", err)
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
		s.Members.Print(true)
	case "list_self":
		ConsolePrintf("Self ID: %s\n", s.ID())
	case "leave":
		s.leaveGroup()
	case "display_suspects":
		s.Members.PrintSuspectedNodes()
	case "display_protocol":
		printCurrentProtocol()
	case "switch":
		if len(parts) != 3 {
			ConsolePrintf("Invalid switch command format. Expected: switch <protocol> <suspicion>\n")
			return
		}
		s.handleSwitch(parts[1], parts[2])
	case "drop":
		if len(parts) != 2 {
			ConsolePrintf("Invalid drop command format. Expected: drop <percentage>\n")
			return
		}
		SetMessageDropRate(parts[1])
	default:
		ConsolePrintf("Unknown command: %s\n", command)
	}
}

func (s *Server) leaveGroup() {
	LogInfo(true, "Initiating graceful leave from the group...")

	// Get current membership snapshot
	snapshot := s.Members.Snapshot()
	selfMember, exists := snapshot[s.ID()]

	if !exists {
		LogError(true, "Error: Self not found in membership list")
		return
	}

	// Mark self as voluntarily leaving
	selfMember.MarkVoluntaryLeave()
	selfMember.Incarnation = s.IncarnationNumber // Use current incarnation
	s.Members.AddOrUpdate(selfMember)

	LogInfo(true, "MEMBER_LEAVE: Marked self as voluntarily leaving: %s", s.ID())
}

func (s *Server) handleSwitch(protocolStr, suspicionStr string) {
	protocolStr = strings.ToLower(protocolStr)
	suspicionStr = strings.ToLower(suspicionStr)

	protocol, ok := ProtocolMap[protocolStr]
	if !ok {
		ConsolePrintf("Invalid protocol '%s'\n", protocolStr)
		return
	}

	suspicion, ok := SuspicionMap[suspicionStr]
	if !ok {
		ConsolePrintf("Invalid suspicion type '%s'\n", suspicionStr)
		return
	}

	//TODO: REVISIT HERE
	SwitchProtocol(protocol, suspicion)

	// Broadcast protocol switch to all other nodes in the membership list
	snapshot := s.Members.Snapshot()

	for _, member := range snapshot {
		// Skip self
		if member.Address != s.Addr {
			go func(nodeAddr string) {
				resp, err := CallProtocolSwitch(nodeAddr, s.ID(), protocol, suspicion, s)
				if err != nil {
					LogError(true, "Failed to send protocol switch to %s: %v", nodeAddr, err)
				} else {
					LogInfo(true, "Sent protocol switch to %s, success: %v", nodeAddr, resp.Success)
				}
			}(member.Address)
		}
	}

	ConsolePrintln("Protocol switch broadcast completed")
}

func printCurrentProtocol() {
	ConsolePrintf("Current Protocol: %s | Suspicion: %s | Message Drop Rate: %.2f%%\n",
		Config.Protocol, Config.Suspicion, Config.MessageDropRate*100)
}

func (s *Server) increaseHeartbeat(heartbeatInterval time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Increment the server's heartbeat counter only for PingAck
		if Config.Protocol == PingAckProtocol {
			continue
		}

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

	resp, err := CallJoin(s.IntroducerAddr, member, s)
	if err != nil {
		LogError(true, "Failed to join cluster: %v", err)
		return err
	} else {
		LogInfo(true, "MEMBER_JOIN: Successfully joined cluster: %s", s.ID())
		// Change to already existing protocol in the group when joining
		Config.Protocol = resp.Protocol
		Config.Suspicion = resp.Suspicion
		mergeMembership(s, resp.MembershipList, resp.IntroducerID)
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
			LogError(true, "Read error: %v", err)
			continue
		}

		// Track bytes received
		if s.BandwidthStats != nil {
			s.BandwidthStats.AddReceived(uint64(n))
		}

		// Implement message drop simulation for testing network failures
		if Config.MessageDropRate > 0.0 {
			// Generate random number between 0.0 and 1.0
			if rand.Float64() < Config.MessageDropRate {
				// Drop the message - simulate network packet loss
				LogInfo(true, "DROPPED message from %s (drop rate: %.2f%%)",
					clientAddr.String(), Config.MessageDropRate*100)
				continue
			}
		}

		// Create a copy of the buffer data for the goroutine to avoid race conditions
		data := make([]byte, n)
		copy(data, buffer[:n])
		go s.handleRPCRequest(conn, clientAddr, data, service)
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
	bytesWritten, _ := conn.WriteToUDP(data, clientAddr)

	// Track bytes sent
	if s.BandwidthStats != nil {
		s.BandwidthStats.AddSent(uint64(bytesWritten))
	}
}

// sendRPCError sends an RPC error response
func (s *Server) sendRPCError(conn *net.UDPConn, clientAddr *net.UDPAddr, id uint64, errMsg string) {
	resp := RPCResponse{
		Error: errMsg,
		ID:    id,
	}
	data, _ := json.Marshal(resp)
	bytesWritten, _ := conn.WriteToUDP(data, clientAddr)

	// Track bytes sent
	if s.BandwidthStats != nil {
		s.BandwidthStats.AddSent(uint64(bytesWritten))
	}
}
