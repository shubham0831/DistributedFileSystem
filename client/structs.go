package main

import (
	"Hackerman/proto/proto"
	"os"
)

type ResponseCommand int64
type WriterAction int64
type FileStatus int64

const (
	LS ResponseCommand = iota
	CD
	MKDIR
	DELETE
	SHAREFILE
	GETFILE
	PWD
	SHOWNODES
)

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
	ChunkedBytes   []byte
	File           *os.File
}

type ControllerMessage struct {
	ResponseCommand    ResponseCommand
	LsResponse         *LsResponse
	CdResponse         *CdResponse
	MkdirResponse      *MkdirResponse
	ShowNodesResponse  *ShowNodesResponse
	DeleteFileResponse *DeleteFileResponse
	ShareFileResponse  *ShareFileResponse
	GetFileResponse    *GetFileResponse
	PwdResponse        *PwdResponse
}

type LsResponse struct {
	ErrorMessage string
	FileList     string
}

type CdResponse struct {
	ErrorMessage string
	NewPath      string
}

type MkdirResponse struct {
	ErrorMessage string
}

type ShowNodesResponse struct {
	ErrorMessage string
	NodeList     string
}

type DeleteFileResponse struct {
	ErrorMessage string
}

type ShareFileResponse struct {
	ErrorMessage  string
	ChunkSize     uint64
	NodeAddresses []*ShareFileNodeAddress
	/**
	add message
	*/
}

type ShareFileNodeAddress struct {
	NodeAddress        string
	FileId             uint64
	ChunkNameAndNumber map[uint64]string
}

type GetFileResponse struct {
	ErrorMessage  string
	FileName      string
	FileSize      uint64
	ChunksPerNode map[string][]*proto.ChunkNameAndNumber
	/**
	add message
	*/
}

type PwdResponse struct {
	ErrorMessage string
	Path         string
}
