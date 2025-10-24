package main

import (
	"context"
	"fmt"

	pb "distributed_log_query/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
	stream, err := client.FileTransfer(context.Background())
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
