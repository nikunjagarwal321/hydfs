package main

import "math/big"

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

type Ack struct {
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

type GetFilesMetadataRequest struct {
	KeyStartRange big.Int `json:"sender_address"`
	KeyEndRange   big.Int `json:"protocol"`
}

type GetFileMetadataResponse struct {
	Success  bool `json:"success"`
	Metadata *Metadata
}

type GetFileMetadataRequest struct {
	Filename string
}

type MultiAppendRequest struct {
	HyDFSFileName string `json:"hydfs_filename"`
	LocalFileName string `json:"local_filename"`
}

type MultiAppendResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type UpdateAppendOrderRequest struct {
	FileName string       `json:"file_name"`
	Appends  []AppendInfo `json:"appends"`
}

type UpdateAppendOrderResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type MergeRequest struct {
	HyDFSFileName string `json:"hydfs_filename"`
}

type MergeResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type ExecuteCommandRequest struct {
	Command string
}

type ExecuteCommandResponse struct {
	Success bool
	Message string
}
