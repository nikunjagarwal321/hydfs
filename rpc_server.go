package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"time"
)

// RPC Service definitions
type DistributedSystemService struct {
	server *Server
}

// Join handles new member joining
func (ds *DistributedSystemService) Join(req *JoinRequest, resp *JoinResponse) error {
	if ds.server.IsIntroducer {
		// Compute hash for the new member
		hashValue := HashToMbits(req.Member.Address)
		req.Member.Hash = hashValue
		req.Member.LastUpdated = time.Now()
		ds.server.Members.AddOrUpdate(req.Member)
		LogInfo(true, "MEMBER_JOIN: Introducer accepted new member: %s with hash: %s", req.Member.ID(), hashValue.String())
		ConsolePrintf("MEMBER_JOIN: Introducer accepted new member: %s with hash: %s\n", req.Member.ID(), hashValue.String())
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

// Ping handles SWIM ping messages
func (ds *DistributedSystemService) Ping(req *PingRequest, resp *Ack) error {
	LogInfo(false, "PingAck: Received PING from %s", req.SenderID)

	// Merge received membership list
	if globalServer != nil {
		mergeMembership(globalServer, req.MembershipList, req.SenderID)
	}

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

// GetFilesMetadata returns filemetadata within that range
func (ds *DistributedSystemService) GetFilesMetadata(req *GetFilesMetadataRequest, resp *GetFileMetadataResponse) error {
	LogInfo(true, "Received GetFilesMetadata request with Keyrange : %s - %s ",
		req.KeyStartRange.String(), req.KeyEndRange.String())

	fileNames := GetFilesWithinRange(ds.server.Metadata.Files, &req.KeyStartRange, &req.KeyEndRange)
	result := make(map[string]FileMetadata)
	for _, name := range fileNames {
		meta := ds.server.Metadata.Files[name]
		result[name] = meta
	}
	resp.Metadata = &Metadata{Files: result}
	resp.Success = true
	LogInfo(true, "Successfully provided metadata for key range: %s - %s", req.KeyStartRange.String(), req.KeyEndRange.String())
	return nil
}

// GetFilesMetadata returns filemetadata within that range
func (ds *DistributedSystemService) GetFileMetadata(req *GetFileMetadataRequest, resp *GetFileMetadataResponse) error {
	LogInfo(true, "Received GetFileMetadata request with Filename : %s ",
		req.Filename)

	fileMetadata, ok := ds.server.Metadata.GetFile(req.Filename)
	if !ok {
		resp.Success = false
		resp.Metadata = &Metadata{Files: make(map[string]FileMetadata)}
		LogInfo(true, "File not found for GetFileMetadata: %s", req.Filename)
		return nil
	}
	resp.Success = true
	resp.Metadata = &Metadata{Files: map[string]FileMetadata{req.Filename: fileMetadata}}
	LogInfo(true, "Successfully provided metadata for file: %s", req.Filename)
	return nil
}

// MultiAppend handles multiappend RPC call - calls handleAppend on the server
func (ds *DistributedSystemService) MultiAppend(req *MultiAppendRequest, resp *MultiAppendResponse) error {
	ConsolePrintf("Received MultiAppend request: HyDFSfilename=%s, LocalFilename=%s\n", req.HyDFSFileName, req.LocalFileName)
	ConsolePrintf("Received MultiAppend request: HyDFSfilename=%s, LocalFilename=%s\n", req.HyDFSFileName, req.LocalFileName)

	// Call handleAppend on the server
	ds.server.handleAppend(req.LocalFileName, req.HyDFSFileName)

	resp.Success = true
	resp.Message = fmt.Sprintf("Append operation completed for %s", req.HyDFSFileName)
	ConsolePrintf("MultiAppend completed: %s\n", resp.Message)
	ConsolePrintf("MultiAppend completed: %s\n", resp.Message)
	return nil
}

// UpdateAppendOrder handles RPC call to update append order for a file
func (ds *DistributedSystemService) UpdateAppendOrder(req *UpdateAppendOrderRequest, resp *UpdateAppendOrderResponse) error {
	LogInfo(true, "Received UpdateAppendOrder request for file: %s", req.FileName)

	// Get current file metadata
	fileMeta, ok := ds.server.Metadata.GetFile(req.FileName)
	if !ok {
		resp.Success = false
		resp.Message = fmt.Sprintf("File %s not found", req.FileName)
		return nil
	}

	// Update the append order
	fileMeta.Appends = req.Appends
	ds.server.Metadata.AddFile(fileMeta)

	resp.Success = true
	resp.Message = fmt.Sprintf("Append order updated for file %s", req.FileName)
	LogInfo(true, "UpdateAppendOrder completed: %s", resp.Message)
	return nil
}

// Merge handles RPC call to execute merge on the primary node
func (ds *DistributedSystemService) Merge(req *MergeRequest, resp *MergeResponse) error {
	LogInfo(true, "Received Merge request for file: %s", req.HyDFSFileName)

	// Execute merge on this node
	ds.server.executeMerge(req.HyDFSFileName)

	resp.Success = true
	resp.Message = fmt.Sprintf("Merge operation completed for file %s", req.HyDFSFileName)
	LogInfo(true, "Merge completed: %s", resp.Message)
	return nil
}

// StartRPCServer starts the RPC server with UDP transport
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

	// Clear hydfs folder on startup
	if err := s.clearHydfsFolder(); err != nil {
		LogError(true, "Failed to clear hydfs folder: %v", err)
		// Continue anyway - this is not a fatal error
	}

	go s.listenForMessages(conn)
	go s.sendTimelyMessagesAsPerProtocol(gossipOrSwimPingInterval)
	go s.increaseHeartbeat(heartbeatInterval)
	go s.backgroundCheckerRPC(suspicionCheckTimeout, suspicionTimeout, deadTimeout, cleanUpTimeout)
	// go s.monitorBandwidth()
	go s.startCLI(cmdChan)
	go s.StartHyDFSGRPCServer()
	go s.garbageCollectorProcess(garbageCollectorInterval)
	go s.mergeMetadataProcess(mergeInterval)

	// main loop processes commands
	for cmd := range cmdChan {
		s.handleCommand(cmd)
	}
	return nil
}

func (s *Server) notifyIntroducer() error {
	member := Member{
		Address:               s.Addr,
		NodeCreationTimestamp: s.NodeCreationTimestamp,
		Status:                StatusAlive,
		Heartbeat:             s.HeartbeatCounter,
		Incarnation:           s.IncarnationNumber,
		LastUpdated:           time.Now(),
		Hash:                  s.Hash, // Include hash
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
	case "GetFilesMetadata":
		var req GetFilesMetadataRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp GetFileMetadataResponse
		rpcErr = service.GetFilesMetadata(&req, &resp)
		result = resp
	case "GetFileMetadata":
		var req GetFileMetadataRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp GetFileMetadataResponse
		rpcErr = service.GetFileMetadata(&req, &resp)
		result = resp

	case "MultiAppend":
		var req MultiAppendRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp MultiAppendResponse
		rpcErr = service.MultiAppend(&req, &resp)
		result = resp

	case "UpdateAppendOrder":
		var req UpdateAppendOrderRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp UpdateAppendOrderResponse
		rpcErr = service.UpdateAppendOrder(&req, &resp)
		result = resp

	case "Merge":
		var req MergeRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp MergeResponse
		rpcErr = service.Merge(&req, &resp)
		result = resp

	case "ExecuteCommand":
		var req ExecuteCommandRequest
		if err := s.convertParams(rpcMsg.Params, &req); err != nil {
			s.sendRPCError(conn, clientAddr, rpcMsg.ID, fmt.Sprintf("invalid params: %v", err))
			return
		}
		var resp ExecuteCommandResponse
		rpcErr = service.ExecuteCommand(&req, &resp)
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

// clearHydfsFolder clears all files in the hydfs directory on startup
func (s *Server) clearHydfsFolder() error {
	hydfsDir := s.FileDirectory
	// Check if directory exists, if not then create and return
	info, err := os.Stat(hydfsDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory doesn't exist, create it
			if err := os.MkdirAll(hydfsDir, 0755); err != nil {
				return fmt.Errorf("failed to create hydfs directory: %w", err)
			}
			LogInfo(true, "Created hydfs directory: %s\n", hydfsDir)
			return nil
		}
		return fmt.Errorf("failed to stat hydfs directory: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("FileDirectory is not a directory: %s", hydfsDir)
	}

	// Read all files in the directory
	entries, err := os.ReadDir(hydfsDir)
	if err != nil {
		return fmt.Errorf("failed to read hydfs directory: %w", err)
	}

	// Remove all files in the directory (no need to check for directories, it is guaranteed that all entries will be files)
	removedCount := 0
	for _, entry := range entries {
		filePath := filepath.Join(hydfsDir, entry.Name())
		if err := os.Remove(filePath); err != nil {
			LogError(true, "Failed to remove file %s: %v", filePath, err)
			continue
		}
		removedCount++
	}

	LogInfo(true, "Cleared hydfs folder: %s (removed %d files)\n", hydfsDir, removedCount)
	return nil
}

func (ds *DistributedSystemService) ExecuteCommand(req *ExecuteCommandRequest, resp *ExecuteCommandResponse) error {
	if ds.server == nil {
		resp.Success = false
		resp.Message = "Server reference is nil"
		return nil
	}

	go ds.server.handleCommand(req.Command) // Run asynchronously
	resp.Success = true
	resp.Message = fmt.Sprintf("Command '%s' sent for execution", req.Command)
	return nil
}
