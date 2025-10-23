package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// ---------------------------
// Struct definitions
// ---------------------------

// Metadata represents the top-level metadata structure.
type Metadata struct {
	Files []FileMetadata `json:"files"`
}

// FileMetadata represents information about a file stored in HyDFS.
type FileMetadata struct {
	FileID       string       `json:"file_id"`
	FileName     string       `json:"file_name"`
	FileHash     int          `json:"file_hash"`
	CreationTime string       `json:"creationTime"`
	Appends      []AppendInfo `json:"appends"`
}

// AppendInfo represents one append operation to a file.
type AppendInfo struct {
	AppendID  string `json:"append_id"`
	Timestamp string `json:"timestamp"`
	Size      int64  `json:"size"`
}

// ---------------------------
// Helper methods
// ---------------------------

// NewFileMetadata creates a new FileMetadata entry.
func NewFileMetadata(name string, hash int) FileMetadata {
	return FileMetadata{
		FileID:       fmt.Sprintf("%s-%d", name, time.Now().UnixNano()),
		FileName:     name,
		FileHash:     hash,
		CreationTime: time.Now().Format("2006-01-02 15:04:05"),
		Appends:      []AppendInfo{},
	}
}

// AddAppend adds a new append record to the file metadata.
func (f *FileMetadata) AddAppend(appendID string, size int64) {
	appendInfo := AppendInfo{
		AppendID:  appendID,
		Timestamp: time.Now().Format("2006-01-02 15:04:05"),
		Size:      size,
	}
	f.Appends = append(f.Appends, appendInfo)
}

// AddFile adds a new file to the metadata.
func (m *Metadata) AddFile(file FileMetadata) {
	m.Files = append(m.Files, file)
}

// ToJSON returns the JSON string representation of Metadata.
func (m *Metadata) ToJSON() (string, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// FromJSON parses a JSON string into the Metadata struct.
func (m *Metadata) FromJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), m)
}
