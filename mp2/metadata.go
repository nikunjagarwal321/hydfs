package main

import (
	"math/big"
	"sync"
	"errors"
)

// Metadata is now concurrency safe with per-file locks
type Metadata struct {
	mu    sync.RWMutex             // map-level protection
	Files map[string]FileMetadata
	locks map[string]*sync.RWMutex // per-file locks
}

// FileMetadata represents information about a file stored in HyDFS.
type FileMetadata struct {
	FileName        string
	FileContentHash string
	FileNameHash    big.Int
	CreationTime    string
	Appends         []AppendInfo
}

// AppendInfo represents one append operation to a file.
type AppendInfo struct {
	FileName        string
	AppendID        string // FileName + ClientTimestamp + ClientID
	ClientID        string
	ClientTimestamp string // Unix time in microseconds (guaranteed increasing by client)
	AppendHash      string
	Size            int64
}

// TODO: add helper functions later if needed
func (fileMeta *FileMetadata) insertAppend(newAppend AppendInfo) {
	inserted := false
	for i, a := range fileMeta.Appends {
		if a.ClientID == newAppend.ClientID &&
			a.ClientTimestamp > newAppend.ClientTimestamp {
			// Insert before this append
			fileMeta.Appends = append(fileMeta.Appends[:i],
				append([]AppendInfo{newAppend}, fileMeta.Appends[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		fileMeta.Appends = append(fileMeta.Appends, newAppend)
	}
}

// getFileLock returns the per-file mutex (helper, not exported)
func (m *Metadata) getFileLock(filename string) *sync.RWMutex {
	m.mu.RLock()
	lock := m.locks[filename]
	m.mu.RUnlock()
	return lock
}

// AddFile adds or overwrites a file (creates per-file lock if missing)
func (m *Metadata) AddFile(meta FileMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Files[meta.FileName] = meta
	if m.locks == nil {
		m.locks = make(map[string]*sync.RWMutex)
	}
	if _, ok := m.locks[meta.FileName]; !ok {
		m.locks[meta.FileName] = &sync.RWMutex{}
	}
}

// GetFile returns a copy (safe for outside use)
func (m *Metadata) GetFile(filename string) (FileMetadata, bool) {
	m.mu.RLock()
	entry, ok := m.Files[filename]
	m.mu.RUnlock()
	if !ok { return FileMetadata{}, false }
	lock := m.getFileLock(filename)
	if lock != nil {
		lock.RLock()
		defer lock.RUnlock()
	}
	return entry, true
}

// AppendToFile safely appends to the file; avoids duplicates
func (m *Metadata) AppendToFile(filename string, appendInfo AppendInfo) error {
	lock := m.getFileLock(filename)
	if lock == nil {
		return errors.New("file does not exist")
	}
	lock.Lock()
	defer lock.Unlock()

	m.mu.RLock()
	fileMeta := m.Files[filename]
	m.mu.RUnlock()

	// Only append if not a duplicate
	for _, a := range fileMeta.Appends {
		if a.AppendID == appendInfo.AppendID {
			return nil // already exists
		}
	}
	fileMeta.insertAppend(appendInfo)

	m.mu.Lock()
	m.Files[filename] = fileMeta
	m.mu.Unlock()
	return nil
}

// ListFiles returns a snapshot of file names
func (m *Metadata) ListFiles() []string {
	m.mu.RLock()
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	m.mu.RUnlock()
	return names
}
