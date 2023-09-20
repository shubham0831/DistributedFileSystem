package main

type ResponseCommand int64

const (
	FileInfoResponse ResponseCommand = iota
	MapResponse
	ReduceResponse
)

type ControllerMessage struct {
	ResponseCommand ResponseCommand
	FileInfo        *FileInfo
	MapResponse     *MapResponseStruct
	ReduceResponse  *ReduceResponseStruct
}

type FileInfo struct {
	ErrorMessage  string
	FileSize      int64
	ChunksPerNode map[string][]string
	NodePerChunk  map[string][]string
}

type MapResponseStruct struct {
	ChunkName          string
	ShufflingComplete  bool
	ErrorMessage       string
	FilesSentToReducer map[string]string
}

type HandleResponseStruct struct {
	NodeIp               string
	MapResponseStruct    *MapResponseStruct
	ReduceResponseStruct *ReduceResponseStruct
	StopLoop             bool
}

type ReduceResponseStruct struct {
	ReducerIp       string
	ReducedFileName string
	ErrorMessage    string
}

type JobAssignmentStruct struct {
	Mappers  map[string]map[string]bool
	Reducers map[string]bool
}
