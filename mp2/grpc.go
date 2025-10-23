package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "distributed_log_query/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// SendFileToNode sends a file to a target node via gRPC streaming
func (s *Server) SendFileToNode(targetAddr string, filename string, data []byte) error {
	// Convert UDP address to gRPC address
	grpcAddr := convertToGRPCAddress(targetAddr)

	// Connect to the target node
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", grpcAddr, err)
	}
	defer conn.Close()

	client := pb.NewHyDFSServiceClient(conn)
	stream, err := client.UploadFile(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create stream: %v", err)
	}

	// Send file in chunks (64KB chunks)
	chunkSize := 64 * 1024
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := &pb.FileChunk{
			Filename: filename,
			Data:     data[i:end],
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send chunk: %v", err)
		}
	}

	// Close and receive response
	status, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %v", err)
	}

	if !status.GetSuccess() {
		return fmt.Errorf("upload failed: %s", status.GetMessage())
	}

	ConsolePrintf("File sent to %s (gRPC: %s): %s\n", targetAddr, grpcAddr, status.GetMessage())
	return nil
}

// convertToGRPCAddress converts UDP address to gRPC address (port + 1000)
func convertToGRPCAddress(udpAddr string) string {
	parts := strings.Split(udpAddr, ":")
	if len(parts) != 2 {
		return udpAddr
	}

	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return udpAddr
	}

	grpcPort := port + 1000
	return fmt.Sprintf("%s:%d", host, grpcPort)
}
