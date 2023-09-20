package main

import (
	"Hackerman/proto/proto"
	"Hackerman/utils"
	"bufio"
	"fmt"
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const ChunkSize = 1024 * 1024 * 64 // 64mb

//const ChunkSize = 12 // for testing purposes only

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

type Client struct {
	serverHandler             *proto.MessageHandler
	serverAddr                string
	computeManagerAddr        string
	logger                    *log.Logger
	scanner                   *bufio.Scanner
	currentDirectory          string
	printChannel              chan string
	controllerResponseChannel chan *ControllerMessage
	writerChannel             chan *WriterMessageStruct
	FileMap                   map[string]*FileStruct
}

func InitClient(serverAddr string, computeManagerAddr string, logger *log.Logger) *Client {
	serverConn, serErr := net.Dial("tcp", serverAddr)
	if serErr != nil {
		logger.Error(serErr.Error())
		logger.Panic("Error connecting to server")
	}

	serverHandler := proto.NewMessageHandler(serverConn)

	client := &Client{
		serverHandler:             serverHandler,
		serverAddr:                serverAddr,
		computeManagerAddr:        computeManagerAddr,
		logger:                    logger,
		scanner:                   bufio.NewScanner(os.Stdin),
		currentDirectory:          "/",
		printChannel:              make(chan string, 100),
		controllerResponseChannel: make(chan *ControllerMessage, 100),
		writerChannel:             make(chan *WriterMessageStruct, 100),
		FileMap:                   make(map[string]*FileStruct),
	}

	return client
}

func main() {
	logger := utils.GetLogger(log.DebugLevel, os.Stdout)
	serverAddr, computeManagerAddr := parseArgs(os.Args, logger)

	client := InitClient(serverAddr, computeManagerAddr, logger)
	client.Start()
}

func (c *Client) Start() {
	/**
	send message (server) -> scan from scanner and send via scanner handler
	receive message (server) -> respond back from server

	send message (nodes) -> once we get response back from server - where to store files, and where to download from -
							we have to connect to the node
	receive message (nodes) -> mainly going to be used for downloading chunks
	*/

	go c.startWriter()
	go c.print()
	go c.listenFromServer()
	c.sendMessagesToServer()
}

func (c *Client) sendMessagesToServer() {
	c.logger.Info("Scanning for messages to be sent to server")
	c.printChannel <- ""

	for {

		if !c.scanner.Scan() {
			continue
		}

		commands := strings.Split(c.scanner.Text(), " ")
		argCount := len(commands)

		/**
		Instruction formats :
			ls {optional path}
			mkdir {filename} {optional target path} // default path is going to be current directory
			cd {target path}
			del {filename} {optional target path} // default path is going to be current directory
			pwd {option path to dir} // default behavior will just print the current directory from the client struct
		*/
		c.dfsCommands(commands, argCount)

	}
}

func (c *Client) dfsCommands(commands []string, argCount int) {
	switch {
	case commands[0] == "ls":
		target := c.currentDirectory
		if argCount == 2 {
			target = filepath.Join(target, commands[1])
		}

		listFile := &proto.ListFiles{Path: target}
		lsWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_ListFiles{ListFiles: listFile}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: lsWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(LS)

		if controllerResponse.LsResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.LsResponse.ErrorMessage
			break
		}

		c.printChannel <- controllerResponse.LsResponse.FileList
		break
	case commands[0] == "mkdir":
		target := c.currentDirectory
		if argCount == 3 {
			target = filepath.Join(target, commands[2])
		}

		makeDir := &proto.MakeDir{TargetDir: target, Name: commands[1]}
		mkdirWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_MakeDir{MakeDir: makeDir}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: mkdirWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(MKDIR)

		if controllerResponse.MkdirResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.MkdirResponse.ErrorMessage
			break
		}

		c.printChannel <- ""
		break
	case commands[0] == "pwd":
		target := c.currentDirectory

		//If target path is not provided, we just print current directory of user
		if argCount == 1 {
			c.printChannel <- c.currentDirectory
			break
		}

		target = filepath.Join(target, commands[1])
		pwd := &proto.PrintWorkingDir{TargetDir: target}
		pwdWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_PrintWorkingDir{PrintWorkingDir: pwd}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: pwdWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(PWD)

		if controllerResponse.PwdResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.PwdResponse.ErrorMessage
			break
		}

		c.printChannel <- controllerResponse.PwdResponse.Path
		break
	case commands[0] == "cd":
		/** TODO */
		changeDir := &proto.ChangeDir{CurrentDir: c.currentDirectory, TargetDir: commands[1]}
		cdWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_ChangeDir{ChangeDir: changeDir}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: cdWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(CD)

		if controllerResponse.CdResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.CdResponse.ErrorMessage
			break
		}

		c.currentDirectory = controllerResponse.CdResponse.NewPath
		c.printChannel <- ""
		break
	case commands[0] == "shownodes":
		showNodes := &proto.ShowNodes{}
		showNodesWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_ShowNodes{ShowNodes: showNodes}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: showNodesWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(SHOWNODES)

		if controllerResponse.ShowNodesResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.ShowNodesResponse.ErrorMessage
			break
		}

		c.printChannel <- controllerResponse.ShowNodesResponse.NodeList
		break
	case commands[0] == "del", commands[0] == "delete":
		target := c.currentDirectory
		if argCount == 3 {
			target = filepath.Join(target, commands[2])
		}
		target = filepath.Join(target, commands[1])

		deleteFile := &proto.DeleteFile{TargetDir: target}
		deleteWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_DeleteFile{DeleteFile: deleteFile}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: deleteWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(DELETE)

		if controllerResponse.DeleteFileResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.DeleteFileResponse.ErrorMessage
			break
		}

		c.printChannel <- ""
		break
	case commands[0] == "share":
		sourceFilePath := commands[1]
		target := c.currentDirectory
		if argCount == 3 {
			target = filepath.Join(target, commands[2])
		}

		fileInfo, statErr := os.Stat(sourceFilePath)
		if statErr != nil {
			c.logger.Errorf("Error getting stat for file %s", commands[1])
			break
		}

		fileStruct := &FileStruct{
			fileName:       fileInfo.Name(),
			fileSize:       uint64(fileInfo.Size()),
			numberOfChunks: 1,
			chunkInfo:      make(map[uint64]*FileChunkInfo),
		}

		if !strings.HasSuffix(fileInfo.Name(), ".txt") {

			currentOffset := int64(0)

			for {
				if currentOffset >= fileInfo.Size() {
					break
				}
				fileChunkInfo := &FileChunkInfo{}
				fileChunkInfo.startingOffset = uint64(currentOffset)
				fileChunkInfo.chunkSent = false
				chunkSize := int64(math.Min(float64(ChunkSize), float64(fileInfo.Size()-currentOffset)))
				fileChunkInfo.chunkSize = uint64(chunkSize)

				fileStruct.chunkInfo[fileStruct.numberOfChunks] = fileChunkInfo
				fileStruct.numberOfChunks++

				currentOffset = currentOffset + chunkSize
			}

		} else {

			file, _ := os.Open(sourceFilePath)
			c.logger.Debugf("User is uplaoding text file")

			// this variable will be mutated quite a lot, can potentially be source of a bug
			currentOffset := int64(0)
			chunkNum := 0
			for {
				if currentOffset >= fileInfo.Size() {
					break
				}

				fileChunkInfo := &FileChunkInfo{}
				fileChunkInfo.startingOffset = uint64(currentOffset)
				fileChunkInfo.chunkSent = false

				c.logger.Debugf("Chunk %d offset starts at %d", chunkNum, currentOffset)
				szToRead := int64(math.Min(float64(ChunkSize), float64(fileInfo.Size()-currentOffset)))

				bytes := getBytes(file, currentOffset, szToRead, c.logger)
				currentOffset = currentOffset + int64(len(bytes))

				for {
					finalByte := bytes[len(bytes)-1]

					// 0x0A is byte for new line
					// checking if the final byte is a newline or not
					// or we might reach the end of the file, either cases we have to break
					if string(finalByte) == "\n" || currentOffset >= fileInfo.Size() {
						break
					}

					file.Seek(currentOffset, 0)
					nextByte := make([]byte, 1) // read the next byte
					file.Read(nextByte)
					bytes = append(bytes, nextByte...)
					currentOffset++
				}

				c.logger.Debugf("Chunk %d offset ends at %d", chunkNum, currentOffset)
				chunkNum++
				fileChunkInfo.chunkSize = uint64(len(bytes))
				fileStruct.chunkInfo[fileStruct.numberOfChunks] = fileChunkInfo
				fileStruct.numberOfChunks++
			}

			file.Close()
		}

		shareFile := &proto.ShareFile{
			TargetDir:  target,
			FileName:   fileStruct.fileName,
			FileSize:   fileStruct.fileSize,
			ChunkCount: fileStruct.numberOfChunks - 1, // chunkCount starts at 1 useful for for loop
		}
		shareFileWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_ShareFile{ShareFile: shareFile}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: shareFileWrapper}}
		c.serverHandler.Send(wrapper)

		controllerResponse := c.waitAndGetResponse(SHAREFILE)

		if controllerResponse.ShareFileResponse.ErrorMessage != "" {
			c.printChannel <- controllerResponse.ShareFileResponse.ErrorMessage
			break
		}

		shareFileResponse := controllerResponse.ShareFileResponse

		go c.shareFile(shareFileResponse, fileStruct, sourceFilePath)
		//
		///**
		//else do something else
		//*/
		//c.printChannel <- ""
		//break
	case commands[0] == "get":
		/**
		get filename (option_filePath)

		if filepath not provided, we use currDir to get targetPath
		if is provided, we join the curr dir with the given path, then we
		join the name as well to get the full path of the file
		*/
		target := c.currentDirectory
		if argCount == 3 {
			target = filepath.Join(target, commands[2])
		}
		target = filepath.Join(target, commands[1])

		getFile := &proto.GetFile{
			TargetPath: target,
		}

		getFileWrapper := &proto.ClientCommands{Commands: &proto.ClientCommands_GetFile{GetFile: getFile}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ClientCommands{ClientCommands: getFileWrapper}}
		c.serverHandler.Send(wrapper)

		getFileResponse := c.waitAndGetResponse(GETFILE).GetFileResponse

		if getFileResponse.ErrorMessage != "" {

			c.printChannel <- getFileResponse.ErrorMessage
			break
		}

		fileName := getFileResponse.FileName
		fileSize := getFileResponse.FileSize
		var numberOfChunks uint64

		for _, val := range getFileResponse.ChunksPerNode {
			numberOfChunks += uint64(len(val))
		}

		c.logger.Debugf("Number of chunks is %d", numberOfChunks)

		file, createFileErr := os.Create(getFileResponse.FileName)
		if createFileErr != nil {
			c.logger.Error("Error in creating file, terminating get file request")
			c.logger.Error(createFileErr.Error())
			break
		}
		// https://stackoverflow.com/questions/16797380/how-to-create-a-10mb-file-filled-with-000000-data-in-golang
		// creating empty file of given size
		file.Truncate(int64(fileSize))
		c.logger.Debugf("Truncated file with size %d", int64(fileSize))

		c.writerChannel <- &WriterMessageStruct{
			WriterAction:   INIT,
			FileName:       fileName,
			FileStatus:     OPEN,
			FileSize:       fileSize,
			ChunkName:      "",
			ChunkedBytes:   nil,
			File:           file,
			NumberOfChunks: numberOfChunks,
		}

		for nodeIp, chunkList := range getFileResponse.ChunksPerNode {
			go func(nodeIp string, chunkList []*proto.ChunkNameAndNumber) {
				nodeConn, nodeErr := net.Dial("tcp", nodeIp)
				if nodeErr != nil {
					c.logger.Errorf("Error connecting to node %s", nodeIp)
				}
				nodeHandler := proto.NewMessageHandler(nodeConn)

				for _, chunkNameAndNumber := range chunkList {
					getChunkRequest := &proto.GetChunkRequest{
						ChunkName:   chunkNameAndNumber.GetChunkName(),
						ChunkNumber: chunkNameAndNumber.GetChunkNumber(),
					}
					getChunkWrapper := &proto.Wrapper{Messages: &proto.Wrapper_GetChunkRequest{GetChunkRequest: getChunkRequest}}
					nodeHandler.Send(getChunkWrapper)

					response, _ := nodeHandler.Receive() //

					// since the node will only respond with one type of message to the client, not adding a switch statement
					getChunkResponse := response.GetGetChunkResponse()

					if getChunkResponse.Error != "" {
						c.printChannel <- "Error in getting " + chunkNameAndNumber.GetChunkName()
						c.printChannel <- getChunkResponse.GetError()
						continue
					}

					c.writerChannel <- &WriterMessageStruct{
						WriterAction:   WRITE,
						FileName:       fileName,
						ChunkName:      chunkNameAndNumber.GetChunkName(),
						ChunkNumber:    chunkNameAndNumber.GetChunkNumber(),
						StartingOffset: chunkNameAndNumber.GetStartingOffset(),
						ChunkedBytes:   getChunkResponse.ChunkedData,
					}

				}

			}(nodeIp, chunkList)
		}

		c.printChannel <- ""
		break
	case commands[0] == "exec":
		pluginPath, dfsFilePath, mapperCount, reducerCount, reducerKey, destPath, maxParallelization := getMRArgs(commands)
		file, _ := os.Open(pluginPath)
		fileStat, _ := file.Stat()
		pluginBytes := getBytes(file, 0, fileStat.Size(), c.logger)
		file.Close()

		mapReduceRequest := &proto.MapReduceRequest{
			FilePath:           dfsFilePath,
			PluginName:         fileStat.Name(),
			Plugin:             pluginBytes,
			NumOfMappers:       mapperCount,
			NumOfReducers:      reducerCount,
			ReducerKey:         reducerKey,
			ServerAddr:         c.serverAddr,
			DestPath:           destPath,
			MaxParallelization: maxParallelization,
		}
		mapReduceRequestWrapper := &proto.ComputeJob{Commands: &proto.ComputeJob_MapReduceRequest{MapReduceRequest: mapReduceRequest}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeJob{ComputeJob: mapReduceRequestWrapper}}

		serverConn, serErr := net.Dial("tcp", c.computeManagerAddr)
		if serErr != nil {
			c.logger.Error(serErr.Error())
			c.logger.Panic("Error connecting to compute manager")
		}

		computeManagerHandler := proto.NewMessageHandler(serverConn)
		computeManagerHandler.Send(wrapper)

		// keep listening till job is complete
	loop:
		for {
			response, err := computeManagerHandler.Receive()

			if err != nil {
				c.logger.Error("Error in receiving message from server (msg below) ")
				c.logger.Error(err.Error())
			}

			switch response.Messages.(type) {
			case *proto.Wrapper_MRStatusUpdate:
				c.logger.Debug("Got a status update")
				statusUpdate := response.GetMRStatusUpdate()
				c.printChannel <- statusUpdate.GetMessage()
				if statusUpdate.JobComplete {
					break loop
				}
			}
		}

	}
}

func getMRArgs(args []string) (string, string, int32, int32, string, string, bool) {
	var (
		pluginPath         string
		dfsFilePath        string
		mapperCount        int32
		reducerCount       int32
		reducerKey         string
		destPath           string
		maxParallelization bool
	)

	maxParallelization = false

	for i := 0; i < len(args); i++ {
		if args[i] == "-plugin" {
			pluginPath = args[i+1]
			i++
		} else if args[i] == "-file" {
			dfsFilePath = args[i+1]
			i++
		} else if args[i] == "-m" || args[i] == "-mapperCount" {
			count, _ := strconv.Atoi(args[i+1])
			mapperCount = int32(count)
			i++
		} else if args[i] == "-r" || args[i] == "-reducerCount" {
			count, _ := strconv.Atoi(args[i+1])
			reducerCount = int32(count)
			i++
		} else if args[i] == "-rKey" || args[i] == "-reducerKey" {
			reducerKey = args[i+1]
			i++
		} else if args[i] == "-d" || args[i] == "-dest" {
			destPath = args[i+1]
			i++
		} else if args[i] == "-maxP" {
			maxParallelization = true
		} else if args[i] == "-maxL" {
			maxParallelization = false
		}

	}

	return pluginPath, dfsFilePath, mapperCount, reducerCount, reducerKey, destPath, maxParallelization
}

func getBytes(file *os.File, offset int64, chunkSize int64, logger *log.Logger) []byte {
	file.Seek(offset, 0)
	chunkedBytes := make([]byte, chunkSize)
	file.Read(chunkedBytes)

	return chunkedBytes
}

func (c *Client) shareFile(shareFileResponse *ShareFileResponse, fileInfoStruct *FileStruct, filePath string) {
	c.logger.Infof("Starting sequence to share file %s", filePath)
	chunkAssignment := shareFileResponse.ChunkAssignment
	startingChunk := int64(1)

	for nodeIp, numberOfChunks := range chunkAssignment {
		go func(nodeIp string, filePath string, fileId uint64, startingChunk int64, numberOfChunks int64, fileInfoStruct *FileStruct) {
			nodeConn, nodeErr := net.Dial("tcp", nodeIp)
			if nodeErr != nil {
				c.logger.Errorf("Error connecting to node %s", nodeIp)
			}
			nodeHandler := proto.NewMessageHandler(nodeConn)

			fileInfo, _ := os.Stat(filePath)
			fileName := fileInfo.Name()
			for i := int64(0); i < numberOfChunks; i++ {
				currentChunk := startingChunk + i
				chunkInfo := fileInfoStruct.chunkInfo[uint64(currentChunk)]
				file, openErr := os.Open(filePath)

				if openErr != nil {
					c.logger.Errorf("Error opening file %s", filePath)
					panic("Unable to open file")
				}

				bytes := getBytes(file, int64(chunkInfo.startingOffset), int64(chunkInfo.chunkSize), c.logger)

				file.Close()

				chunkName := strings.Split(fileName, ".")[0]
				chunkName += fmt.Sprint(currentChunk)

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
				c.logger.Debugf("Sent chunk %s to node %s\n", chunkInfo, nodeIp)
			}

		}(nodeIp, filePath, shareFileResponse.FileId, startingChunk, numberOfChunks, fileInfoStruct)
		startingChunk = startingChunk + numberOfChunks
	}

}

/**
Method just waits for appropriate type of response to come and then returns that response
*/
func (c *Client) waitAndGetResponse(command ResponseCommand) *ControllerMessage {
	for {
		res := <-c.controllerResponseChannel
		if res.ResponseCommand != command {
			c.controllerResponseChannel <- res
			continue
		}
		return res
	}
}

func (c *Client) listenFromServer() {
	c.logger.Info("Listening for messages from server")

	for {
		response, err := c.serverHandler.Receive()

		if err != nil {
			c.logger.Error("Error in receiving message from server (msg below) ")
			c.logger.Error(err.Error())
		}

		/**
		Responses are being handled where they were made, this is to make the code easier to read
		*/
	parentSwitch:
		switch response.Messages.(type) {
		case *proto.Wrapper_CommandResponse:
			switch response.GetCommandResponse().Responses.(type) {
			case *proto.CommandResponse_LsResponse:
				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand: LS,
					LsResponse: &LsResponse{
						ErrorMessage: response.GetCommandResponse().GetLsResponse().GetErrorMessage(),
						FileList:     response.GetCommandResponse().GetLsResponse().GetFileList(),
					},
				}
				break parentSwitch
			case *proto.CommandResponse_PwdResponse:
				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand: PWD,
					PwdResponse: &PwdResponse{
						ErrorMessage: response.GetCommandResponse().GetPwdResponse().GetErrorMessage(),
						Path:         response.GetCommandResponse().GetPwdResponse().GetDirectoryPath(),
					},
				}
				break parentSwitch
			case *proto.CommandResponse_CdResponse:
				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand: CD,
					CdResponse: &CdResponse{
						ErrorMessage: response.GetCommandResponse().GetCdResponse().GetErrorMessage(),
						NewPath:      response.GetCommandResponse().GetCdResponse().GetNewPath(),
					},
				}
				break parentSwitch
			case *proto.CommandResponse_MkdirResponse:
				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand: MKDIR,
					MkdirResponse:   &MkdirResponse{ErrorMessage: response.GetCommandResponse().GetMkdirResponse().GetErrorMessage()},
				}
				break parentSwitch
			case *proto.CommandResponse_ShowNodesResponse:
				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand: SHOWNODES,
					ShowNodesResponse: &ShowNodesResponse{
						ErrorMessage: response.GetCommandResponse().GetShowNodesResponse().GetErrorMessage(),
						NodeList:     response.GetCommandResponse().GetShowNodesResponse().GetNodeList(),
					},
				}
				break parentSwitch
			case *proto.CommandResponse_ShareFileResponse:
				shareFileResponse := &ShareFileResponse{
					ErrorMessage:    response.GetCommandResponse().GetShareFileResponse().GetErrorMessage(),
					ChunkAssignment: response.GetCommandResponse().GetShareFileResponse().GetChunkPerNode(),
					FileId:          response.GetCommandResponse().GetShareFileResponse().GetFileId(),
				}

				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand:   SHAREFILE,
					ShareFileResponse: shareFileResponse,
				}

				break parentSwitch
			case *proto.CommandResponse_GetFileResponse:
				getFileResponse := &GetFileResponse{
					ErrorMessage:  response.GetCommandResponse().GetShareFileResponse().GetErrorMessage(),
					FileName:      response.GetCommandResponse().GetGetFileResponse().GetFileName(),
					FileSize:      response.GetCommandResponse().GetGetFileResponse().GetFileSize(),
					ChunksPerNode: nil,
				}

				nodeList := response.GetCommandResponse().GetGetFileResponse().GetChunksPerNode()
				chunksPerNodeMap := make(map[string][]*proto.ChunkNameAndNumber)
				for _, chunksPerNode := range nodeList {
					chunksPerNodeMap[chunksPerNode.GetNodeIp()] = chunksPerNode.ChunkNameAndNumber
				}

				getFileResponse.ChunksPerNode = chunksPerNodeMap

				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand: GETFILE,
					GetFileResponse: getFileResponse,
				}
				break parentSwitch
			case *proto.CommandResponse_DeleteFileResponse:
				c.controllerResponseChannel <- &ControllerMessage{
					ResponseCommand:    DELETE,
					DeleteFileResponse: &DeleteFileResponse{ErrorMessage: response.GetCommandResponse().GetDeleteFileResponse().GetErrorMessage()},
				}
				break parentSwitch
			}
		}
	}
}

func (c *Client) print() {
	/**
	TODO update printChannel struct so it know whether message is an error or not
	and prints them appropriately
	*/
	for {
		printString := <-c.printChannel

		if printString == "" {
			fmt.Println()
			style := color.New(color.FgCyan, color.Bold)
			style.Printf("%s -> ", c.currentDirectory)
			continue
		}

		fmt.Printf("\n%s\n", printString)

		style := color.New(color.FgCyan, color.Bold)
		style.Printf("%s: ", c.currentDirectory)
	}
}

func (c *Client) startWriter() {

	type WriteInfo struct {
		FileStatus     FileStatus
		FileSize       uint64
		NumberOfChunks uint64
		File           *os.File
		ChunkWritten   map[string]bool // will be used to keep track of whether we have written this chunk
	}

	writeMap := make(map[string]*WriteInfo)

	for {
		writeMessage := <-c.writerChannel
		if writeMessage.WriterAction == INIT {
			c.logger.Debugf("Got message to init file %s size is %d and number of chunks is %d", writeMessage.FileName, writeMessage.FileSize, writeMessage.NumberOfChunks)
			writeMap[writeMessage.FileName] = &WriteInfo{
				FileStatus:     writeMessage.FileStatus,
				FileSize:       writeMessage.FileSize,
				File:           writeMessage.File,
				NumberOfChunks: writeMessage.NumberOfChunks,
				ChunkWritten:   make(map[string]bool),
			}

			continue
		}

		writeInfo := writeMap[writeMessage.FileName]
		c.logger.Debugf("For chunk %d starting offset is %d and size is %d", writeMessage.ChunkNumber-1, writeMessage.StartingOffset, len(writeMessage.ChunkedBytes))
		startingIndex := int64(writeMessage.StartingOffset)
		writeInfo.File.Seek(startingIndex, 0)
		writeInfo.File.Write(writeMessage.ChunkedBytes)
		s, _ := writeInfo.File.Stat()
		c.logger.Debugf("For chunk %d starting offset is %d and size after write is %d", writeMessage.ChunkNumber-1, writeMessage.StartingOffset, s.Size())
		writeInfo.ChunkWritten[writeMessage.ChunkName] = true

		if uint64(len(writeInfo.ChunkWritten)) == writeInfo.NumberOfChunks {
			writeInfo.File.Close()
			delete(writeMap, writeMessage.FileName) // delete this entry from the map
			c.printChannel <- "Done receiving file"
		}
	}
}

func parseArgs(args []string, logger *log.Logger) (string, string) {
	//logger.Info("Parsing command line arguments")

	var (
		serverAddr         string
		computeManagerAddr string
	)

	for i := 0; i < len(args); i++ {
		if serverAddr == "" && (args[i] == "-sa" || args[i] == "-serverAddr") {
			serverAddr = args[i+1]
			i++
		} else if computeManagerAddr == "" && (args[i] == "-cm" || args[i] == "-computeManager") {
			computeManagerAddr = args[i+1]
			i++
		}
	}

	return serverAddr, computeManagerAddr
}
