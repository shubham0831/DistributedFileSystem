package main

import "time"

type Operation int64
type StructField int64
type ValueType int64

const (
	Put Operation = iota
	Get
	GetResult
)

const (
	LastHeartBeat StructField = iota
	IsActive
	UsedSpace
	NodePointer
)

const (
	Int64 ValueType = iota
	Uint64
	Pointer
	Bool
	Time
	String
)

type GetStruct struct {
	Request  *GetRequest
	Response *GetResponse
}

type GetRequest struct {
	StructField StructField
}

type GetResponse struct {
	StructField StructField
	Value       *MapValue
}

// -------

type PutStruct struct {
	StructField StructField
	Value       *MapValue
}

// --------

type NodeMapOperation struct {
	Operation Operation
	Key       string
	Get       *GetStruct
	Put       *PutStruct
}

type MapValue struct {
	Int64       int64
	Uint64      uint64
	NodePointer *Node
	Bool        bool
	Time        time.Time
	String      string
}

type ChunkReceivedMessage struct {
	Error       string
	ChunkNumber uint64
}
