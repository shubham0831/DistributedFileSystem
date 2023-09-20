package main

import (
	"Hackerman/proto/proto"
	"Hackerman/utils"
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"math"
	"net"
	"os"
	"plugin"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Node struct {
	// not really using id for anything, but it's still good to have
	// might need it for later projects
	id                     string
	listener               net.Listener
	listenerAddress        string
	serverHandler          *proto.MessageHandler
	logger                 *log.Logger
	registrationChannel    chan *RegistrationMessage
	sendChannel            chan *proto.Wrapper
	acknowledgementChannel chan *proto.Wrapper
	dbLock                 *sync.Mutex // need this to prevent database is locked error
	totalRequest           int64
	jobMap                 map[string]*MapReduceHandler
	filesToReduce          map[string][]*ReduceFileInfo
	writerChannel          chan *WriterMessageStruct
	serverAddr             string
}

type ReduceFileInfo struct {
	fileName string
	ready    bool
	empty    bool
}

// this is me very tired, these structs are exactly the same as the ones in client.go

type FileChunkInfo struct {
	startingOffset uint64
	chunkSize      uint64
	chunkSent      bool
}

type FileStruct struct {
	fileName       string
	fileSize       uint64
	numberOfChunks uint64
	chunkInfo      map[uint64]*FileChunkInfo
}

type ShareFileResponse struct {
	ErrorMessage    string
	ChunkAssignment map[string]int64
	FileId          uint64
	/**
	add message
	*/
}

const SORT_BUFFER_SIZE = 1024 * 1024 * 1024 // 1GB

func InitNode(listenerAddr string, serverAddr string, logger *log.Logger) *Node {
	// this is where the storage node listens to requests from the user or other storage nodes
	listenConn, lisErr := net.Listen("tcp", listenerAddr)
	// this is where storage node communicates with the server
	serverConn, serErr := net.Dial("tcp", serverAddr)

	logger.Debugf("Lis Addr %s and Server Addr is %s", listenerAddr, serverAddr)
	if lisErr != nil {
		logger.Error(lisErr.Error())
		logger.Panic("Error setting up listener")
	}

	if serErr != nil {
		logger.Error(serErr.Error())
		logger.Panic("Error connecting to server")
	}

	serverHandler := proto.NewMessageHandler(serverConn)

	node := &Node{
		listener:               listenConn,
		listenerAddress:        listenerAddr,
		serverHandler:          serverHandler,
		logger:                 logger,
		registrationChannel:    make(chan *RegistrationMessage, 100),
		sendChannel:            make(chan *proto.Wrapper, 100),
		acknowledgementChannel: make(chan *proto.Wrapper, 100),
		totalRequest:           0,
		jobMap:                 make(map[string]*MapReduceHandler),
		writerChannel:          make(chan *WriterMessageStruct, 100),
		filesToReduce:          make(map[string][]*ReduceFileInfo),
		serverAddr:             serverAddr,
	}

	return node
}

func main() {
	logger := utils.GetLogger(log.DebugLevel, os.Stdout)
	listenerAddr, serverAddr := parseArgs(os.Args, logger)

	node := InitNode(listenerAddr, serverAddr, logger)
	node.start()
}

func (n *Node) start() {
	n.logger.Infof("Starting Node")
	//go n.sendWrapper()
	go n.startWriter()
	go n.listenFromServer()
	go n.RegisterAndSendHeartBeats()

	for {
		if conn, err := n.listener.Accept(); err == nil {
			reqHandler := proto.NewMessageHandler(conn)
			go n.handleRequest(reqHandler)
		}
		/**
		node listener code will go here
		*/
	}
}

func (n *Node) handleRequest(reqHandler *proto.MessageHandler) {
	for {
		request, err := reqHandler.Receive()

		if err != nil {
			n.logger.Error("Error in receiving message from server/client")
			n.logger.Error(err.Error())
			continue
		}

		switch request.Messages.(type) {
		case *proto.Wrapper_SendReplicaRequest:
			chunkName := request.GetSendReplicaRequest().GetChunkName()
			targetIp := request.GetSendReplicaRequest().GetTargetIp()

			fileBytes, readErr := os.ReadFile(chunkName)
			if readErr != nil {
				n.logger.Error("Error opening chunk file")
				n.logger.Error(readErr.Error())
			}

			targetConnection, connectionErr := net.Dial("tcp", targetIp)
			targetHandler := proto.NewMessageHandler(targetConnection)

			if connectionErr != nil {
				n.logger.Errorf("Error in establishing connection with node %s", targetIp)
				n.logger.Error(connectionErr.Error())
				continue
			}

			chunkedFile := &proto.ChunkedFile{
				FileId:         request.GetSendReplicaRequest().GetFileId(),
				ChunkName:      chunkName,
				ChunkNumber:    request.GetSendReplicaRequest().GetChunkNumber(),
				ChunkSize:      request.GetSendReplicaRequest().GetChunkSize(),
				ChunkedFile:    fileBytes,
				StartingOffset: request.GetSendReplicaRequest().GetStartingOffset(),
				Replication:    request.GetSendReplicaRequest().GetReplication(),
			}

			n.totalRequest++
			chunkedFileWrapper := &proto.Wrapper{Messages: &proto.Wrapper_ChunkedFile{ChunkedFile: chunkedFile}}
			targetHandler.Send(chunkedFileWrapper)

			break
		case *proto.Wrapper_ChunkedFile: // when node gets a chunked file from a client
			chunkName := request.GetChunkedFile().GetChunkName()
			fileBytes := request.GetChunkedFile().GetChunkedFile()
			expectedFileSize := request.GetChunkedFile().GetChunkSize()
			fileId := request.GetChunkedFile().GetFileId()
			startingOffset := request.GetChunkedFile().GetStartingOffset()

			n.logger.Debugf("File id gotten is %d", fileId)

			chunkReceivedAck := &proto.ChunkReceivedAck{}

			if uint64(len(fileBytes)) != expectedFileSize {
				n.logger.Errorf("Size of chunk %s is not same as expected, expected = %d, actual = %d", chunkName, expectedFileSize, len(fileBytes))
				chunkReceivedAck.ErrorMessage = "File size is not same as expected file size"
				chunkReceivedAck.ChunkSize = request.GetChunkedFile().GetChunkSize()
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ChunkedReceivedAck{ChunkedReceivedAck: chunkReceivedAck}}
				n.serverHandler.Send(wrapper)
				break
			}

			// 0777 will allow all users read and write access to all files and folders within that directory
			writeErr := os.WriteFile(chunkName, fileBytes, 0644)

			if writeErr != nil {
				n.logger.Errorf("Error writing chunk %s", chunkName)
				n.logger.Error(writeErr.Error())
				chunkReceivedAck.ErrorMessage = "Error writing chunk"
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ChunkedReceivedAck{ChunkedReceivedAck: chunkReceivedAck}}
				n.serverHandler.Send(wrapper)
				break
			}

			chunkReceivedAck.ChunkName = chunkName
			chunkReceivedAck.ChunkNumber = request.GetChunkedFile().GetChunkNumber()
			chunkReceivedAck.ChunkSize = expectedFileSize
			chunkReceivedAck.FileId = fileId
			chunkReceivedAck.StartingOffset = startingOffset
			chunkReceivedAck.Replication = request.GetChunkedFile().GetReplication()

			wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ChunkedReceivedAck{ChunkedReceivedAck: chunkReceivedAck}}
			n.serverHandler.Send(wrapper)
			break
		case *proto.Wrapper_GetChunkRequest:
			getChunkResponse := &proto.GetChunkResponse{ChunkName: request.GetGetChunkRequest().GetChunkName()}
			fileBytes, readErr := os.ReadFile(request.GetGetChunkRequest().GetChunkName())
			if readErr != nil {
				n.logger.Error(readErr.Error())
				getChunkResponse.Error = "Node is not able to open file"
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_GetChunkResponse{GetChunkResponse: getChunkResponse}}
				reqHandler.Send(wrapper)
			}

			getChunkResponse.ChunkedData = fileBytes
			n.totalRequest++
			wrapper := &proto.Wrapper{Messages: &proto.Wrapper_GetChunkResponse{GetChunkResponse: getChunkResponse}}
			reqHandler.Send(wrapper)
		case *proto.Wrapper_ComputeCommands:
			switch request.GetComputeCommands().Commands.(type) {
			case *proto.ComputeCommands_RunJobRequest:
				switch request.GetComputeCommands().GetRunJobRequest().Request.(type) {
				case *proto.RunJobRequest_MapRequest:
					go n.handleMapRequest(request.GetComputeCommands().GetRunJobRequest().GetMapRequest(), reqHandler)
				case *proto.RunJobRequest_ChunkedMappedFile:
					n.writerChannel <- &WriterMessageStruct{
						WriterAction:   INIT,
						FileName:       request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetFileName(),
						FileStatus:     0,
						NumberOfChunks: uint64(request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetNumberOfChunks()),
						JobId:          request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetJobId(),
					}

					n.writerChannel <- &WriterMessageStruct{
						WriterAction:   WRITE,
						FileName:       request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetFileName(),
						ChunkName:      request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetChunkName(),
						ChunkNumber:    uint64(request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetChunkNumber()),
						StartingOffset: uint64(request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetStartingOffset()),
						ChunkedBytes:   request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetChunkedFile(),
						JobId:          request.GetComputeCommands().GetRunJobRequest().GetChunkedMappedFile().GetJobId(),
					}
				case *proto.RunJobRequest_ReduceRequest:
					go n.handleReduceRequest(request.GetComputeCommands().GetRunJobRequest().GetReduceRequest(), reqHandler)
				case *proto.RunJobRequest_PluginDownload:
					jobId := request.GetComputeCommands().GetRunJobRequest().GetPluginDownload().GetJobId()
					pluginName := request.GetComputeCommands().GetRunJobRequest().GetPluginDownload().GetPluginName()
					pluginBytes := request.GetComputeCommands().GetRunJobRequest().GetPluginDownload().GetPlugin()

					os.WriteFile(pluginName, pluginBytes, 0777)
					plug, err := plugin.Open("./" + request.GetComputeCommands().GetRunJobRequest().GetPluginDownload().GetPluginName())

					if err != nil {
						fmt.Println(err.Error())
					}

					if _, present := n.jobMap[jobId]; !present {
						n.jobMap[jobId] = &MapReduceHandler{}
					}
					n.jobMap[request.GetComputeCommands().GetRunJobRequest().GetPluginDownload().GetJobId()].plugin = plug
					n.logger.Infof("Reducer only node has received plugin")
				}
			}
		}
	}
}

func (n *Node) handleReduceRequest(reduceRequest *proto.ReduceRequest, reqHandler *proto.MessageHandler) {
	jobId := reduceRequest.JobId
	reducerFileMap := reduceRequest.ReducerFileMap
	filesToReduce := n.filesToReduce[jobId]

	n.logger.Debugf("Got a reduce request, files to be reduced are\n")
	for _, f := range filesToReduce {
		n.logger.Debugln(f.fileName)
	}

	for {
		exit := false
		for _, fileName := range reducerFileMap[n.listenerAddress].GetFileNames() {
			for _, fileName2 := range filesToReduce {
				if fileName != fileName2.fileName {
					continue
				}
				exit = true
			}
		}

		if exit {
			break
		}
	}

	n.logger.Info("Node has gotten all the files it needs to reduce and can now start reducing")

	unsortedOutputFileName := "unsortedMergedReducedFile.txt"
	unsortedCombinedFiles, _ := os.Create(unsortedOutputFileName)

	for _, fileInfo := range filesToReduce {
		f, _ := os.Open(fileInfo.fileName)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			unsortedCombinedFiles.WriteString(scanner.Text() + "\n")
		}
	}

	unsortedCombinedFiles.Close()

	sortedFileName := "sortedFileToBeReduced.txt"

	StartSort(unsortedOutputFileName, SORT_BUFFER_SIZE, sortedFileName, n.logger)

	n.logger.Info("Sorting complete, starting reduce")

	plugin := n.jobMap[jobId].plugin

	sortedFile, _ := os.Open("./" + sortedFileName)
	defer sortedFile.Close()
	scanner := bufio.NewScanner(sortedFile)

	machineName := strings.Split(n.listenerAddress, ".")[0]
	reducedFileName := machineName + "ReducedFile.txt"
	n.logger.Infof("Reduced file name is %s", reducedFileName)
	reducedFile, _ := os.Create("./" + reducedFileName)
	defer reducedFile.Close()

	for scanner.Scan() {
		line := scanner.Text()
		splitLine := strings.Split(line, " ")
		key := splitLine[0]
		valueArray := splitLine[1:]
		countArr := make([]int, 0)

		for _, str := range valueArray {
			if str != "" {
				num, cErr := strconv.Atoi(str)
				if cErr != nil {
					n.logger.Errorf("Error converting string to int, string is %s", str)
					continue
				}
				countArr = append(countArr, num)
			}
		}

		reducer, _ := plugin.Lookup("Reduce")
		rKey, rOut := reducer.(func(string, []int) (string, int64))(key, countArr)
		outputLine := rKey + " " + strconv.FormatInt(rOut, 10) + "\n"
		//n.logger.Infof("Output from reducer is %s\n", outputLine)
		reducedFile.WriteString(outputLine)
	}

	n.logger.Infof("Done reducing file")

	n.uploadFileToServer(reducedFileName, reducedFile, reduceRequest.GetTargetPath())

	reduceResponse := &proto.ReduceResponse{
		FileName:     reducedFileName,
		NodeIp:       n.listenerAddress,
		ErrorMessage: "",
	}

	runJobResponse := &proto.RunJobResponse{Response: &proto.RunJobResponse_ReduceResponse{ReduceResponse: reduceResponse}}
	computeCommands := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobResponse{RunJobResponse: runJobResponse}}
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: computeCommands}}
	reqHandler.Send(wrapper)

	n.logger.Info("Reducing is complete, sent confirmation")
}

func (n *Node) uploadFileToServer(reducedFileName string, reducedFile *os.File, targetPathInDfs string) {

	reducedFileInfo, statErr := os.Stat(reducedFileName)
	if statErr != nil {
		n.logger.Errorf("Error getting stat for file %s", reducedFileName)
		return
	}

	fileStruct := &FileStruct{
		fileName:       reducedFileInfo.Name(),
		fileSize:       uint64(reducedFileInfo.Size()),
		numberOfChunks: 1,
		chunkInfo:      make(map[uint64]*FileChunkInfo),
	}

	// this variable will be mutated quite a lot, can potentially be source of a bug
	currentOffset := int64(0)
	chunkNum := 0
	for {
		if currentOffset >= reducedFileInfo.Size() {
			break
		}

		fileChunkInfo := &FileChunkInfo{}
		fileChunkInfo.startingOffset = uint64(currentOffset)
		fileChunkInfo.chunkSent = false

		n.logger.Debugf("Chunk %d offset starts at %d", chunkNum, currentOffset)
		szToRead := int64(math.Min(float64(ChunkSize), float64(reducedFileInfo.Size()-currentOffset)))

		bytes := getBytes(reducedFile, currentOffset, szToRead, n.logger)
		currentOffset = currentOffset + int64(len(bytes))

		for {
			finalByte := bytes[len(bytes)-1]

			// 0x0A is byte for new line
			// checking if the final byte is a newline or not
			// or we might reach the end of the file, either cases we have to break
			if string(finalByte) == "\n" || currentOffset >= reducedFileInfo.Size() {
				break
			}

			reducedFile.Seek(currentOffset, 0)
			nextByte := make([]byte, 1) // read the next byte
			reducedFile.Read(nextByte)
			bytes = append(bytes, nextByte...)
			currentOffset++
		}

		n.logger.Debugf("Chunk %d offset ends at %d", chunkNum, currentOffset)
		chunkNum++
		fileChunkInfo.chunkSize = uint64(len(bytes))
		fileStruct.chunkInfo[fileStruct.numberOfChunks] = fileChunkInfo
		fileStruct.numberOfChunks++
	}

	shareFile := &proto.ShareFile{
		TargetDir:  targetPathInDfs,
		FileName:   fileStruct.fileName,
		FileSize:   fileStruct.fileSize,
		ChunkCount: fileStruct.numberOfChunks - 1, // chunkCount starts at 1 useful for for loop
	}

	shareFileWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_ShareFile{ShareFile: shareFile}}
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: shareFileWrapper}}

	serverConn, serErr := net.Dial("tcp", n.serverAddr)
	if serErr != nil {
		n.logger.Error(serErr.Error())
		n.logger.Panic("Error connecting to server")
	}

	serverHandler := proto.NewMessageHandler(serverConn)
	serverHandler.Send(wrapper)

	var controllerResponse *ShareFileResponse
	breakForLoop := false

	n.logger.Infof("Sent controller request for chunked file")
	for {
		response, err := serverHandler.Receive()

		if err != nil {
			n.logger.Error("Error in receiving message from server (msg below) ")
			n.logger.Error(err.Error())
		}

		switch response.Messages.(type) {
		case *proto.Wrapper_CommandResponse:
			switch response.GetCommandResponse().Responses.(type) {
			case *proto.CommandResponse_ShareFileResponse:
				controllerResponse = &ShareFileResponse{
					ErrorMessage:    response.GetCommandResponse().GetShareFileResponse().GetErrorMessage(),
					ChunkAssignment: response.GetCommandResponse().GetShareFileResponse().GetChunkPerNode(),
					FileId:          response.GetCommandResponse().GetShareFileResponse().GetFileId(),
				}
				breakForLoop = true
			}
		}

		if breakForLoop {
			break
		}
	}

	if controllerResponse.ErrorMessage != "" {
		n.logger.Errorf("Error in shareFileRequest %s", controllerResponse.ErrorMessage)
		return
	}

	n.logger.Infof("Got response for sharing file from server")

	n.shareFile(controllerResponse, fileStruct, reducedFileName)

	n.logger.Infof("Done with sharing file")

}

func (n *Node) shareFile(shareFileResponse *ShareFileResponse, fileInfoStruct *FileStruct, filePath string) {
	n.logger.Infof("Starting sequence to share file %s", filePath)
	chunkAssignment := shareFileResponse.ChunkAssignment
	startingChunk := int64(1)

	for nodeIp, numberOfChunks := range chunkAssignment {
		// no go routine over here
		func(nodeIp string, filePath string, fileId uint64, startingChunk int64, numberOfChunks int64, fileInfoStruct *FileStruct) {
			nodeConn, nodeErr := net.Dial("tcp", nodeIp)
			if nodeErr != nil {
				n.logger.Errorf("Error connecting to node %s", nodeIp)
			}
			nodeHandler := proto.NewMessageHandler(nodeConn)

			fileInfo, _ := os.Stat(filePath)
			fileName := fileInfo.Name()
			for i := int64(0); i < numberOfChunks; i++ {
				currentChunk := startingChunk + i
				chunkInfo := fileInfoStruct.chunkInfo[uint64(currentChunk)]
				file, openErr := os.Open(filePath)

				if openErr != nil {
					n.logger.Errorf("Error opening file %s", filePath)
					panic("Unable to open file")
				}

				bytes := getBytes(file, int64(chunkInfo.startingOffset), int64(chunkInfo.chunkSize), n.logger)

				file.Close()

				// our file name is something long and weird
				chunkName := fileName + fmt.Sprint(currentChunk)
				n.logger.Infof("Sending chunk %s", chunkName)
				chunkedFileMessage := &proto.ChunkedFile{
					FileId:         fileId,
					ChunkName:      chunkName,
					ChunkNumber:    uint64(currentChunk),
					ChunkSize:      chunkInfo.chunkSize,
					StartingOffset: chunkInfo.startingOffset,
					ChunkedFile:    bytes,
					Replication:    true,
				}

				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ChunkedFile{ChunkedFile: chunkedFileMessage}}
				sendErr := nodeHandler.Send(wrapper)

				if sendErr != nil {
					panic(fmt.Sprintf("Error sending %s to node %s\n", chunkName, nodeIp))
				}
				chunkInfo.chunkSent = true
				n.logger.Debugf("Sent chunk %s to node %s\n", chunkInfo, nodeIp)
			}

		}(nodeIp, filePath, shareFileResponse.FileId, startingChunk, numberOfChunks, fileInfoStruct)
		startingChunk = startingChunk + numberOfChunks
	}
}

func (n *Node) handleMapRequest(mapRequest *proto.MapRequest, reqHandler *proto.MessageHandler) {
	mapReduceHandler := InitMRHandler(n.listenerAddress, n.logger, reqHandler, mapRequest)
	n.jobMap[mapRequest.GetJobId()] = mapReduceHandler
	mapReduceHandler.StartMapping()

	mapReduceHandler.UpdateFileMetadata()

	sentFileMap := mapReduceHandler.Shuffle()
	filesSent := make([]*proto.FileSentToReducer, 0)

	for nodeIp, fileName := range sentFileMap {
		msg := &proto.FileSentToReducer{
			ReducerIp: nodeIp,
			FileName:  fileName,
		}

		if nodeIp == n.listenerAddress {
			n.filesToReduce[mapReduceHandler.jobId] = append(n.filesToReduce[mapReduceHandler.jobId], &ReduceFileInfo{
				fileName: fileName,
				ready:    true,
				empty:    false,
			})
		}

		filesSent = append(filesSent, msg)
	}

	mapResponse := &proto.MapResponse{
		ChunkName:          "",
		ShufflingComplete:  true,
		FilesSentToReducer: filesSent,
		ErrorMessage:       "",
	}

	runJobResponse := &proto.RunJobResponse{Response: &proto.RunJobResponse_MapResponse{MapResponse: mapResponse}}
	computeCommands := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobResponse{RunJobResponse: runJobResponse}}
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: computeCommands}}
	reqHandler.Send(wrapper)

	n.logger.Info("Shuffling is complete, sent confirmation")

}

func (n *Node) listenFromServer() {
	n.logger.Info("Listening to messages from server")
	for {
		receivedMessage, receiveErr := n.serverHandler.Receive()

		if receiveErr != nil {
			n.logger.Error("Error in receiving message from server (message next line)")
			n.logger.Error(receiveErr.Error())
			continue
		}

		switch receivedMessage.Messages.(type) {
		case *proto.Wrapper_RegistrationAck:
			received := &ReceivedRegistration{
				Accepted: receivedMessage.GetRegistrationAck().GetAccepted(),
				Id:       receivedMessage.GetRegistrationAck().GetId(),
				Message:  receivedMessage.GetRegistrationAck().GetMessage(),
			}

			n.registrationChannel <- &RegistrationMessage{
				MessageState:         Received,
				ReceivedRegistration: received,
			}
			break
		}
	}
}

func (n *Node) RegisterAndSendHeartBeats() {
	msg := n.getRegistrationMessage()

	// all messages will be sent through the same channel, since we only have one handler
	// TODO change it if it turns out to be a not so good idea
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_Registration{Registration: msg}}

	err := n.serverHandler.Send(wrapper)
	if err != nil {
		n.logger.Error("Error sending message to server")
		n.logger.Error(err.Error())
	}

	ack := <-n.registrationChannel

	receivedRegistration := ack.ReceivedRegistration

	if !receivedRegistration.Accepted {
		n.logger.Error("Server did not accept our registration, exiting (message below)")
		n.logger.Error(receivedRegistration.Message)
		return
	}

	n.id = receivedRegistration.Id
	n.logger.Infof("Registration successful, id is %s", n.id)

	n.logger.Info("Starting to send heartbeats")
	for {
		heartBeat := &proto.HeartBeat{
			Id:           n.id,
			Alive:        true,
			UsedSpace:    n.getDiskSpace(),
			TotalRequest: n.totalRequest,
		}

		hbWrapper := &proto.Wrapper{Messages: &proto.Wrapper_HeartBeat{HeartBeat: heartBeat}}
		hbError := n.serverHandler.Send(hbWrapper)
		if hbError != nil {
			n.logger.Error("Error sending message to server")
			n.logger.Error(err.Error())
		}
		time.Sleep(5 * time.Second)
	}
}

func parseArgs(args []string, logger *log.Logger) (string, string) {
	logger.Info("Parsing command line arguments")

	var (
		listenerAddr string
		serverAddr   string
	)

	for i := 0; i < len(args); i++ {
		if listenerAddr == "" && (args[i] == "-la" || args[i] == "-listenerAddr") {
			listenerAddr = args[i+1]
			i++
		} else if serverAddr == "" && (args[i] == "-sa" || args[i] == "serverAddr") {
			serverAddr = args[i+1]
			i++
		}
	}

	return listenerAddr, serverAddr
}

func (n *Node) getDiskSpace() uint64 {
	var stat unix.Statfs_t
	wd, _ := os.Getwd()
	statErr := unix.Statfs(wd, &stat)
	if statErr != nil {
		n.logger.Error("Error getting stats for current working dir (message below)")
		n.logger.Error(statErr.Error())
		panic("Panic")
	}
	size := stat.Bavail * uint64(stat.Bsize)
	return size
}

func (n *Node) getRegistrationMessage() *proto.Registration {
	msg := &proto.Registration{
		ConnectionAddress: n.listenerAddress,
		TotalSize:         n.getDiskSpace(),
	}
	return msg
}

func (n *Node) sendWrapper() {
	for {
		wrapper := <-n.sendChannel
		err := n.serverHandler.Send(wrapper)
		if err != nil {
			n.logger.Error("Error sending message to server")
			n.logger.Error(err.Error())
		}
	}
}

func (n *Node) startWriter() {

	type WriteInfo struct {
		FileStatus     FileStatus
		FileSize       uint64
		NumberOfChunks uint64
		File           *os.File
		ChunkWritten   map[string]bool // will be used to keep track of whether we have written this chunk
	}

	writeMap := make(map[string]*WriteInfo)

	for {
		writeMessage := <-n.writerChannel
		if writeMessage.WriterAction == INIT {
			if _, present := writeMap[writeMessage.FileName]; !present {
				//n.logger.Debugf("Got message to init file %s size is %d and number of chunks is %d", writeMessage.FileName, writeMessage.FileSize, writeMessage.NumberOfChunks)
				file, _ := os.Create(writeMessage.FileName)
				writeMap[writeMessage.FileName] = &WriteInfo{
					FileStatus:     writeMessage.FileStatus,
					File:           file,
					NumberOfChunks: writeMessage.NumberOfChunks,
					ChunkWritten:   make(map[string]bool),
				}
			}
			continue
		}
		//
		//if writeMessage.ChunkedBytes == nil || len(writeMessage.ChunkedBytes) == 0 || int32(len(writeMessage.FileSize)) == 0 {
		//	continue
		//}

		writeInfo := writeMap[writeMessage.FileName]
		//n.logger.Debugf("For chunk %d starting offset is %d and size is %d", writeMessage.ChunkNumber-1, writeMessage.StartingOffset, len(writeMessage.ChunkedBytes))
		startingIndex := int64(writeMessage.StartingOffset)
		writeInfo.File.Seek(startingIndex, 0)
		writeInfo.File.Write(writeMessage.ChunkedBytes)
		//s, _ := writeInfo.File.Stat()
		//n.logger.Debugf("For chunk %d starting offset is %d and size after write is %d", writeMessage.ChunkNumber-1, writeMessage.StartingOffset, s.Size())
		writeInfo.ChunkWritten[writeMessage.ChunkName] = true

		if uint64(len(writeInfo.ChunkWritten)) == writeInfo.NumberOfChunks {
			n.logger.Infof("Reducer got file %s", writeMessage.FileName)

			if _, present := n.filesToReduce[writeMessage.JobId]; !present {
				n.filesToReduce[writeMessage.JobId] = make([]*ReduceFileInfo, 0)
			}

			n.filesToReduce[writeMessage.JobId] = append(n.filesToReduce[writeMessage.JobId], &ReduceFileInfo{
				fileName: writeMessage.FileName,
				ready:    true,
			})
			//n.filesToReduce[writeMessage.FileName] = false
			writeInfo.File.Close()
			delete(writeMap, writeMessage.FileName) // delete this entry from the map
		}
	}
}
