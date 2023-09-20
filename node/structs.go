package main

import "os"

type MessageState int64

const (
	Sent MessageState = iota
	Received
)

func (s MessageState) String() string {
	switch s {
	case Sent:
		return "Sent"
	case Received:
		return "Received"
	}
	return "unknown"
}

type RegistrationMessage struct {
	MessageState         MessageState
	ReceivedRegistration *ReceivedRegistration
}

type ReceivedRegistration struct {
	Accepted bool
	Id       string
	Message  string
}

type JobStruct struct {
	isReducer   bool
	reduceList  []string
	chunksToMap map[string]*MappedChunkInfo
}

type MappedChunkInfo struct {
	chunkName       string
	mappedChunkName string // what name to store the mapped file as
	mapped          bool
}

type SyncMessage int64

const (
	Mapped SyncMessage = iota
	Written
)

type WriteSyncMessage struct {
	syncMessage SyncMessage
	number      int32
}

type WriterAction int64
type FileStatus int64

const (
	INIT WriterAction = iota
	WRITE
)

const (
	OPEN FileStatus = iota
	CLOSED
)

type WriterMessageStruct struct {
	WriterAction   WriterAction
	FileName       string
	FileSize       uint64
	FileStatus     FileStatus
	NumberOfChunks uint64
	ChunkName      string
	ChunkNumber    uint64
	StartingOffset uint64
	ChunkedBytes   []byte
	File           *os.File
	JobId          string
}
