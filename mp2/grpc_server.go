package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"

	pb "distributed_log_query/proto"

	"google.golang.org/grpc"
)

// HyDFSServer implements the gRPC HyDFSService
type HyDFSServer struct {
	pb.UnimplementedHyDFSServiceServer
	server *Server
}

// UploadFile handles streaming file upload
func (h *HyDFSServer) UploadFile(stream grpc.ClientStreamingServer[pb.FileChunk, pb.UploadStatus]) error {
	var filename string
	var fileData []byte

	// Receive file chunks from the client
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// End of stream - save the file
			break
		}
		if err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("Error receiving chunk: %v", err),
			})
		}

		// First chunk contains the filename
		if filename == "" {
			filename = chunk.GetFilename()
		}

		// Append data
		fileData = append(fileData, chunk.GetData()...)
	}

	// Save the file
	if err := h.saveFile(filename, fileData); err != nil {
		return stream.SendAndClose(&pb.UploadStatus{
			Success: false,
			Message: fmt.Sprintf("Error saving file: %v", err),
		})
	}

	ConsolePrintf("FILE RECEIVED: %s (%d bytes)\n", filename, len(fileData))

	return stream.SendAndClose(&pb.UploadStatus{
		Success: true,
		Message: fmt.Sprintf("File %s received successfully", filename),
	})
}

// saveFile saves the file to the hydfs directory
func (h *HyDFSServer) saveFile(filename string, data []byte) error {
	// Create hydfs directory if it doesn't exist
	hydfsDir := "hydfs"
	if err := os.MkdirAll(hydfsDir, 0755); err != nil {
		return fmt.Errorf("failed to create hydfs directory: %v", err)
	}

	// Write file
	filePath := filepath.Join(hydfsDir, filename)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}

// StartHyDFSGRPCServer starts the gRPC server for HyDFS
func (s *Server) StartHyDFSGRPCServer() {
	// Parse UDP address and create gRPC address (UDP port + 1000)
	udpAddr := s.Addr
	host, portStr, err := net.SplitHostPort(udpAddr)
	if err != nil {
		LogError(true, "Failed to parse UDP address: %v", err)
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		LogError(true, "Failed to parse port: %v", err)
		return
	}

	grpcPort := port + 1000
	grpcAddr := fmt.Sprintf("%s:%d", host, grpcPort)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		LogError(true, "Failed to listen on gRPC port %s: %v", grpcAddr, err)
		return
	}

	grpcServer := grpc.NewServer()
	pb.RegisterHyDFSServiceServer(grpcServer, &HyDFSServer{server: s})

	ConsolePrintf("HyDFS gRPC server listening on %s\n", grpcAddr)

	if err := grpcServer.Serve(lis); err != nil {
		LogError(true, "Failed to serve gRPC: %v", err)
	}
}
