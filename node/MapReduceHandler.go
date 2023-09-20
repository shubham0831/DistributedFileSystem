package main

import (
	"Hackerman/proto/proto"
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hash/fnv"
	"math"
	"net"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
	"time"
)

const ChunkSize = 1024 * 1024 * 64 // 64mb
//const ChunkSize = 12

type MapReduceHandler struct {
	nodeIp             string
	logger             *log.Logger
	computeManagerConn *proto.MessageHandler
	mapRequest         *proto.MapRequest
	jobId              string
	chunksToBeMapped   []string
	reducerFilesMap    map[int32]*FileInfo
	plugin             *plugin.Plugin
	contextChan        chan map[string]int
	writerCloseChan    chan bool
	writeSyncStruct    *WriteSyncStruct
	writeSyncChan      chan *WriteSyncMessage
	debugChan          chan *DebugMessage
	numberOfMappers    int32
	handlerChan        chan string
}

type DebugMessage struct {
	line string
	out  map[string]int
}

type FileInfo struct {
	reducerIp      string
	file           *os.File
	fileSize       int64
	filePath       string
	numberOfChunks int32
	chunkInfo      map[int32]*ChunkInfo
}

type ChunkInfo struct {
	startingOffset int64
	chunkSize      int64
}

// WriteSyncStruct
// Ran into the issue that once the user defined map had been called for each line in a chunk, before the writer can finish writing out
// all the info, the files are closed and the method returns, so some lines are not written. This struct keeps a track of the number of lines
// that have been mapped, and the number of lines that have been written. Once the 2 are the same, that's when we close the files
type WriteSyncStruct struct {
	linesMapped  int32
	linesWritten int32
}

func InitMRHandler(nodeIp string, logger *log.Logger, reqHandler *proto.MessageHandler, mapRequest *proto.MapRequest) *MapReduceHandler {
	mapReduceHandler := &MapReduceHandler{}
	mapReduceHandler.nodeIp = nodeIp
	mapReduceHandler.logger = logger
	mapReduceHandler.computeManagerConn = reqHandler
	mapReduceHandler.jobId = mapRequest.GetJobId()
	mapReduceHandler.chunksToBeMapped = mapRequest.GetChunkNames()

	os.WriteFile(mapRequest.GetPluginName(), mapRequest.GetPlugin(), 0777)
	plug, err := plugin.Open("./" + mapRequest.GetPluginName())

	if err != nil {
		fmt.Println(err.Error())
	}

	mapReduceHandler.plugin = plug
	mapReduceHandler.reducerFilesMap = initMappedFiles(nodeIp, mapRequest.GetReducers(), mapRequest.GetJobId(), logger)
	mapReduceHandler.contextChan = make(chan map[string]int, 100)

	mapReduceHandler.debugChan = make(chan *DebugMessage, 100)
	mapReduceHandler.writeSyncStruct = &WriteSyncStruct{
		linesMapped:  0,
		linesWritten: 0,
	}

	mapReduceHandler.writeSyncChan = make(chan *WriteSyncMessage)

	mapReduceHandler.numberOfMappers = mapRequest.NumberOfMappers

	for _, reducer := range mapRequest.GetReducers() {
		if reducer == nodeIp {
			mapReduceHandler.numberOfMappers--
			break
		}
	}

	return mapReduceHandler
}

func initMappedFiles(nodeIp string, reducerList []string, jobId string, logger *log.Logger) map[int32]*FileInfo {
	reducerFileMap := make(map[int32]*FileInfo)
	reduceNum := int32(0)
	for _, reducer := range reducerList {
		machineName := strings.Split(reducer, ".")[0]
		fileName := machineName+ "MappedOutput" + ".txt"
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			panic("Error creating " + fileName)
		}
		//file.Close() // file will be closed once mapping is done // so at the start of the shuffler phase?

		absFilePath, _ := filepath.Abs("./" + fileName)
		logger.Debugf("Absolute file path is %s", absFilePath)
		reducerFileMap[reduceNum] = &FileInfo{
			reducerIp:      reducer,
			file:           file,
			filePath:       absFilePath,
			chunkInfo:      make(map[int32]*ChunkInfo),
			numberOfChunks: 0,
		}
		reduceNum++
	}

	return reducerFileMap
}

func (h *MapReduceHandler) startDebug() {
	for {
		out := <-h.debugChan
		h.logger.Errorf("output for line %s is ", out.line)
		h.logger.Error(out.out)
	}
}

func (h *MapReduceHandler) StartMapping() {

	go h.startWriteSync()
	go h.startContextWriter()
	go h.startDebug()

	/**
	No need for a wait group since we will be waiting till all files are written
	*/
	var wg sync.WaitGroup
	for _, chunkName := range h.chunksToBeMapped {
		wg.Add(1)
		go func(fileName string) {
			h.mapChunk(fileName)
			wg.Done()
		}(chunkName)
	}

	wg.Wait()
	time.Sleep(time.Second * 10)
	// wait until we have written all the lines which have been mapped
	for {
		if h.writeSyncStruct.linesMapped == h.writeSyncStruct.linesWritten {
			//h.logger.Error("Lines mapped and written")
			break
		}
	}

	h.logger.Infof("All mappers are done writing, closing the files now")

	for _, fileInfo := range h.reducerFilesMap {
		fileInfo.file.Close()
	}
}

func (h *MapReduceHandler) UpdateFileMetadata() {
	for _, fileInfo := range h.reducerFilesMap {
		fileStat, _ := os.Stat(fileInfo.filePath)
		currentOffset := int64(0)
		fileInfo.fileSize = fileStat.Size()

		if fileInfo.fileSize == 0 {
			continue
		}

		for {
			if currentOffset >= fileStat.Size() {
				break
			}

			fileInfo.chunkInfo[fileInfo.numberOfChunks] = &ChunkInfo{
				startingOffset: currentOffset,
				chunkSize:      int64(math.Min(float64(ChunkSize), float64(fileStat.Size()-currentOffset))),
			}
			currentOffset = currentOffset + fileInfo.chunkInfo[fileInfo.numberOfChunks].chunkSize
			fileInfo.numberOfChunks++
		}
	}
}

func (h *MapReduceHandler) GetFilesToBeReducedLocally() string {

	for _, fileInfo := range h.reducerFilesMap {
		if fileInfo.reducerIp == h.nodeIp && fileInfo.fileSize > 0 {
			return fileInfo.filePath
		}
	}

	return ""
}

// Shuffle // returns a list of string which are the name of files which this node has
// will only be populated if this node is one of the reducers
func (h *MapReduceHandler) Shuffle() map[string]string {

	filesSentTo := make(map[string]string)

	for _, fileInfo := range h.reducerFilesMap {
		if fileInfo.reducerIp == h.nodeIp {
			mappedFile, _ := os.Open(fileInfo.filePath)
			fileStats, _ := mappedFile.Stat()
			if fileStats.Size() <= 0 {
				mappedFile.Close()
				continue
			}
			filesSentTo[fileInfo.reducerIp] = fileStats.Name()
			continue
		}

		// send file to reducer
		reducerHandler := getConnection(fileInfo.reducerIp, h.logger)
		mappedFile, _ := os.Open(fileInfo.filePath)
		fileStats, _ := mappedFile.Stat()

		if fileStats.Size() <= 0 {
			continue
		}

		h.logger.Infof("Sending chunkedMapped file %s to %s", fileInfo.filePath, fileInfo.reducerIp)
		for i := int32(0); i < fileInfo.numberOfChunks; i++ {
			chunkName := strings.Split(fileStats.Name(), ".")[0]
			chunkName += fmt.Sprint(i + 1)
			chunkedBytes := getBytes(mappedFile, fileInfo.chunkInfo[i].startingOffset, fileInfo.chunkInfo[i].chunkSize, h.logger)

			chunkedMappedFile := &proto.ChunkedMappedFile{
				FileName:       fileStats.Name(),
				JobId:          h.jobId,
				NumberOfChunks: fileInfo.numberOfChunks,
				ChunkName:      chunkName,
				ChunkNumber:    i + 1,
				ChunkSize:      int64(len(chunkedBytes)),
				ChunkedFile:    chunkedBytes,
				StartingOffset: fileInfo.chunkInfo[i].startingOffset,
			}

			runJobRequest := &proto.RunJobRequest{Request: &proto.RunJobRequest_ChunkedMappedFile{ChunkedMappedFile: chunkedMappedFile}}

			chunkedMappedFileWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobRequest{RunJobRequest: runJobRequest}}
			wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: chunkedMappedFileWrapper}}

			reducerHandler.Send(wrapper)

			filesSentTo[fileInfo.reducerIp] = fileStats.Name()
		}

		mappedFile.Close()
	}

	return filesSentTo
}

func (h *MapReduceHandler) startWriteSync() {
	for {
		syncMessage := <-h.writeSyncChan

		if syncMessage.syncMessage == Mapped {
			h.writeSyncStruct.linesMapped += syncMessage.number
		} else if syncMessage.syncMessage == Written {
			h.writeSyncStruct.linesWritten += syncMessage.number
		}
	}
}

func (h *MapReduceHandler) mapChunk(chunkName string) {
	chunkedFile, err := os.Open("./" + chunkName)
	defer chunkedFile.Close()
	if err != nil {
		panic("Error in reading chunk " + chunkName)
	}

	//h.logger.Errorf("Mapping chunk %s", chunkName)
	scanner := bufio.NewScanner(chunkedFile)

	/**
	Issue is that before the context chan can finish the writing of the files, all the lines in all the chunks have been
	mapped
	*/
	lineNumber := 0
	for scanner.Scan() {
		line := scanner.Text()
		//h.logger.Infof("Scanning line %s", line)
		mapper, _ := h.plugin.Lookup("Map")
		cMap := mapper.(func(int, string) map[string]int)(lineNumber, line)

		//h.debugChan <- &DebugMessage{
		//	line: line,
		//	out:  cMap,
		//}

		h.contextChan <- cMap
		lineNumber++

		h.writeSyncChan <- &WriteSyncMessage{
			syncMessage: Mapped,
			number:      int32(1),
		}
	}

	mapResponse := &proto.MapResponse{
		ChunkName:         chunkName,
		ShufflingComplete: false,
		ErrorMessage:      "",
	}
	runJobResponse := &proto.RunJobResponse{Response: &proto.RunJobResponse_MapResponse{MapResponse: mapResponse}}
	computeCommands := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobResponse{RunJobResponse: runJobResponse}}
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: computeCommands}}
	h.computeManagerConn.Send(wrapper)
}

func (h *MapReduceHandler) startContextWriter() {

	for {
		mapOutput := <-h.contextChan
		//h.logger.Infof("Map output is")

		for key, value := range mapOutput {
			//h.logger.Infof("Key = %s and value = %d", key, value)
			if key == "" {

				//h.logger.Infof("Key %s with value %s is nil", key, value)
				continue
			}

			outputLine := key + " " + fmt.Sprint(value) + "\n"
			//h.logger.Infof("Output line is %s", outputLine)

			hashFunc := fnv.New32a()
			hashFunc.Write([]byte(key))
			hashValue := int32(hashFunc.Sum32())

			modulo := int32(math.Abs(float64(hashValue % int32(len(h.reducerFilesMap)))))
			//h.logger.Infof("Modulo is %d", modulo)

			reducerFile := h.reducerFilesMap[modulo]

			//h.logger.Infof("Writing to reducer %s", h.reducerFilesMap[modulo].reducerIp)
			//h.logger.Infof("------")
			reducerFile.file.WriteString(outputLine)
		}

		h.writeSyncChan <- &WriteSyncMessage{
			syncMessage: Written,
			number:      1,
		}
	}
}

func getBytes(file *os.File, offset int64, chunkSize int64, logger *log.Logger) []byte {
	file.Seek(offset, 0)
	chunkedBytes := make([]byte, chunkSize)
	file.Read(chunkedBytes)

	return chunkedBytes
}

func getConnection(address string, logger *log.Logger) *proto.MessageHandler {
	serverConn, serErr := net.Dial("tcp", address)
	if serErr != nil {
		logger.Panicf("Error establishing connection to %s", address)
	}

	//logger.Infof("Established connection to %s", address)
	handler := proto.NewMessageHandler(serverConn)
	return handler
}

func (h *MapReduceHandler) empty() {
	//mapper, _ := plug.Lookup("Test")
	//p := mapper.(func(int) int)(100)
	//logger.Errorf("Output from plugin is %d", p)

	/**
	save plugin and establish connection to it, then add that to the handler

	for reducer, reducerFile := range mapReduceHandler.reducerFilesMap {
		logger.Debugf("Opening file for reducer %s, fileName is %s", reducer, reducerFile)
		f, _ := os.OpenFile(reducerFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
		f.WriteString("Hello from the other side")
	}
	*/
}
