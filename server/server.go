package main

import (
	"Hackerman/filesystem"
	"Hackerman/proto/proto"
	"Hackerman/utils"
	"database/sql"
	log "github.com/sirupsen/logrus"
	"math/rand"
	_ "modernc.org/sqlite"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const MinimumReplicas = 3

type Server struct {
	db                      *sql.DB
	logger                  *log.Logger
	listener                net.Listener
	listenerAddress         string
	fs                      *filesystem.FileSystem
	nodeMap                 map[string]*Node
	nodeMapOperationChannel chan *NodeMapOperation
	chunkSize               uint64
	chunkReceivedChannel    chan *ChunkReceivedMessage
	dbLock                  *sync.Mutex // need this to prevent database is locked error
}

func InitServer(listenAddr string, dbPath string, logger *log.Logger, freshTable bool) *Server {
	logger.Infof("Initializing server at %s", listenAddr)

	db, dbErr := sql.Open("sqlite", dbPath)
	if dbErr != nil {
		logger.Error("Error connecting to DB")
	}
	//db.SetMaxIdleConns(2000)
	//db.SetConnMaxIdleTime(time.Second * 20)
	db.SetMaxOpenConns(100)
	logger.Infof("Connection to db established")

	dbLock := &sync.Mutex{}
	fs, nodeMap, ok := filesystem.GetFileSystem(db, logger, dbLock, freshTable)
	if !ok {
		panic("Unable init file system")
	}
	logger.Infof("Init file system succesful")

	listener, listenErr := net.Listen("tcp", listenAddr)

	if listenErr != nil {
		logger.Panic("Error setting up listener")
	}

	server := &Server{
		db:                      db,
		logger:                  logger,
		listener:                listener,
		listenerAddress:         listenAddr,
		fs:                      fs,
		nodeMap:                 make(map[string]*Node),
		nodeMapOperationChannel: make(chan *NodeMapOperation, 100),
		chunkSize:               1024 * 1024 * 100,
		chunkReceivedChannel:    make(chan *ChunkReceivedMessage, 500),
		dbLock:                  dbLock,
	}

	if nodeMap != nil {
		nMap := convertToNodeMap(nodeMap, db, logger, dbLock)
		server.nodeMap = nMap
	}

	return server
}

func convertToNodeMap(nMap map[string]*filesystem.NodeR, db *sql.DB, logger *log.Logger, dbLock *sync.Mutex) map[string]*Node {
	nodeMap := make(map[string]*Node)

	for key, val := range nMap {
		nodeMap[key] = &Node{
			Id:                val.Id,
			Active:            val.Active,
			ConnectionAddress: val.ConnectionAddress,
			db:                db,
			LastHeartBeat:     time.Time{},
			TotalSize:         val.TotalSize,
			UsedSpace:         val.UsedSpace,
			logger:            logger,
			dbLock:            dbLock,
			TotalRequest:      val.TotalRequest,
		}
	}

	return nodeMap
}

func main() {
	logger := utils.GetLogger(log.DebugLevel, os.Stdout)
	listenerAddr, dbPath, freshTable := parseArgs(os.Args, logger)

	server := InitServer(listenerAddr, dbPath, logger, freshTable)
	server.start()
}

func (s *Server) start() {

	//go s.handleReplication()
	s.logger.Debug("Starting listener loop")
	for {
		if conn, err := s.listener.Accept(); err == nil {
			reqHandler := proto.NewMessageHandler(conn)
			go s.handleRequest(reqHandler)
		} else {
			s.logger.Error(err.Error())
			s.logger.Error("Unable to accept request")
		}
	}
}

func (s *Server) handleRequest(reqHandler *proto.MessageHandler) {
	for {
		request, err := reqHandler.Receive()

		if err != nil {
			s.logger.Error("Error in receiving message from node/client (message next line)")
			s.logger.Error(err.Error())
			continue
		}

		//s.logger.Info("Got a request")

	parentSwitch:
		switch request.Messages.(type) {
		case *proto.Wrapper_Registration:
			/**
			Only a node will be sending a registration request and have a heartbeat, unique id
			and stuff like that, so all that is going to be handled in a separate function. We return when
			that function returns
			*/
			s.handleNode(reqHandler, request)
			return
		case *proto.Wrapper_ClientCommands:
			/**
			Sqlite is ACID compliant, so not adding safety for concurrent writes
			*/
			switch request.GetClientCommands().Commands.(type) {
			case *proto.ClientCommands_ListFiles:
				fileList, ok := s.fs.LS(request.GetClientCommands().GetListFiles().GetPath())
				lsResponse := &proto.LsResponse{}

				if !ok {
					lsResponse.FileList = ""
					lsResponse.ErrorMessage = "Error : probably not valid directory"
				} else {
					lsResponse.FileList = fileList
					lsResponse.ErrorMessage = ""
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_LsResponse{LsResponse: lsResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)

				break parentSwitch
			case *proto.ClientCommands_PrintWorkingDir:
				ok := s.fs.CheckDirExist(request.GetClientCommands().GetPrintWorkingDir().GetTargetDir())
				pwdResponse := &proto.PwdResponse{}

				if !ok {
					pwdResponse.DirectoryPath = ""
					pwdResponse.ErrorMessage = "Error : probably not valid directory"
				} else {
					pwdResponse.DirectoryPath = request.GetClientCommands().GetPrintWorkingDir().GetTargetDir()
					pwdResponse.ErrorMessage = ""
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_PwdResponse{PwdResponse: pwdResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)

				break parentSwitch
			case *proto.ClientCommands_MakeDir:
				ok := s.fs.MKDIR(request.GetClientCommands().GetMakeDir().GetTargetDir(), request.GetClientCommands().GetMakeDir().GetName())
				mkdirResponse := &proto.MkdirResponse{}

				if !ok {
					mkdirResponse.ErrorMessage = "Error : probably not valid target directory"
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_MkdirResponse{MkdirResponse: mkdirResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)
				break parentSwitch
			case *proto.ClientCommands_ChangeDir:
				newPath, ok := s.fs.CD(request.GetClientCommands().GetChangeDir().GetCurrentDir(), request.GetClientCommands().GetChangeDir().GetTargetDir())
				cdResponse := &proto.CdResponse{}

				if !ok {
					cdResponse.NewPath = ""
					cdResponse.ErrorMessage = "Error -> probably not valid target directory"
				} else {
					cdResponse.NewPath = newPath
					cdResponse.ErrorMessage = ""
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_CdResponse{CdResponse: cdResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)

				break parentSwitch
			case *proto.ClientCommands_ShowNodes:
				nodeList, ok := PrintNodes(s.db, s.logger, s.dbLock, s.nodeMap)
				showNodesResponse := &proto.ShowNodesResponse{}

				if !ok {
					showNodesResponse.NodeList = ""
					showNodesResponse.ErrorMessage = "Unable to show nodes"
				} else {
					showNodesResponse.NodeList = nodeList
					showNodesResponse.ErrorMessage = ""
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_ShowNodesResponse{ShowNodesResponse: showNodesResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)

				break parentSwitch
			case *proto.ClientCommands_ShareFile:
				shareFileResponse := &proto.ShareFileResponse{}

				targetDir := request.GetClientCommands().GetShareFile().GetTargetDir()
				fileName := request.GetClientCommands().GetShareFile().GetFileName()
				fileSize := request.GetClientCommands().GetShareFile().GetFileSize()
				chunkCount := request.GetClientCommands().GetShareFile().GetChunkCount()

				targetExist := s.fs.CheckDirExist(targetDir)
				if !targetExist {
					shareFileResponse.ErrorMessage = "Target dir does not exist"
					responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_ShareFileResponse{ShareFileResponse: shareFileResponse}}
					wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
					reqHandler.Send(wrapper)
					break parentSwitch
				}

				filePathExist := s.fs.CheckDirExist(filepath.Join(targetDir, fileName))
				if filePathExist {
					shareFileResponse.ErrorMessage = "File with similar name in dir already exists"
					responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_ShareFileResponse{ShareFileResponse: shareFileResponse}}
					wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
					reqHandler.Send(wrapper)
					break parentSwitch
				}

				availableNodes := GetAvailableIps(s.nodeMap, s.logger)
				chunkAssignment := make(map[string]int64)
				rand.Seed(time.Now().Unix()) // initialize global pseudo random generator

				for i := uint64(0); i < chunkCount; i++ {
					randomNode := availableNodes[rand.Intn(len(availableNodes))]
					if _, present := chunkAssignment[randomNode]; !present {
						chunkAssignment[randomNode] = 0
					}
					chunkAssignment[randomNode]++
				}

				// id of file in the database
				fileId, insertOk := s.fs.InsertFile(fileName, filepath.Join(targetDir, fileName), chunkCount, targetDir, fileSize, "IN PROGRESS")
				s.logger.Debugf("File id which server is sending to client is %d", fileId)

				if !insertOk {
					shareFileResponse.ErrorMessage = "Error in inserting file into table"
					responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_ShareFileResponse{ShareFileResponse: shareFileResponse}}
					wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
					reqHandler.Send(wrapper)
					break parentSwitch
				}

				s.logger.Infof("File successfully stored in database, id is %d", fileId)
				shareFileResponse.FileId = fileId
				shareFileResponse.ChunkPerNode = chunkAssignment
				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_ShareFileResponse{ShareFileResponse: shareFileResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)

				break parentSwitch
			case *proto.ClientCommands_GetFile:
				getFileResponse := &proto.GetFileResponse{}
				targetPath := request.GetClientCommands().GetGetFile().GetTargetPath()

				// function returns name only for logging purpose
				ready, fileExistsErr, name, fileId, size := s.fs.CheckFileReadyToDownload(targetPath)

				if !ready {
					getFileResponse.ErrorMessage = fileExistsErr
					responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_GetFileResponse{GetFileResponse: getFileResponse}}
					wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
					reqHandler.Send(wrapper)
					break parentSwitch
				}

				/**
				map[chunkNumber] -> {chunkName, []nodeIps}
				*/
				fileChunkInfo, ok := s.fs.GetFileChunkAddresses(fileId)

				if !ok {
					getFileResponse.ErrorMessage = "Unable to get chunk addresses for file " + name
					responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_GetFileResponse{GetFileResponse: getFileResponse}}
					wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
					reqHandler.Send(wrapper)
					break parentSwitch
				}

				// map[nodeIPs] > []chunksName
				targetNodes := make(map[string][]*proto.ChunkNameAndNumber)

				for chunkNumber, val := range fileChunkInfo {
					chunkName := val.ChunkName
					startingOffset := val.StartingOffset
					randomIp := val.NodeIps[rand.Intn(len(val.NodeIps))]

					if _, exists := targetNodes[randomIp]; !exists {
						targetNodes[randomIp] = make([]*proto.ChunkNameAndNumber, 0)
					}

					chunkNameAndNumber := &proto.ChunkNameAndNumber{
						ChunkName:      chunkName,
						ChunkNumber:    chunkNumber,
						StartingOffset: startingOffset,
					}

					targetNodes[randomIp] = append(targetNodes[randomIp], chunkNameAndNumber)
				}

				getFileResponse.FileName = name
				getFileResponse.FileSize = size
				getFileResponse.ChunksPerNode = make([]*proto.ChunksPerNode, 0)

				for nodeIp, chunkNameList := range targetNodes {
					chunksPerNode := &proto.ChunksPerNode{}
					chunksPerNode.NodeIp = nodeIp
					chunksPerNode.ChunkNameAndNumber = chunkNameList
					getFileResponse.ChunksPerNode = append(getFileResponse.ChunksPerNode, chunksPerNode)
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_GetFileResponse{GetFileResponse: getFileResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}

				s.logger.Error("Sending following response for get file request")
				s.logger.Error(responseWrapper)

				reqHandler.Send(wrapper)

				break parentSwitch
			case *proto.ClientCommands_DeleteFile:
				targetDir := request.GetClientCommands().GetDeleteFile().GetTargetDir()
				ok, errMessage := s.fs.DeleteFile(targetDir) // errMessage will be empty if no errors
				deleteFileResponse := &proto.DeleteFileResponse{
					Ok:           ok,
					ErrorMessage: errMessage,
				}

				responseWrapper := &proto.CommandResponse{Responses: &proto.CommandResponse_DeleteFileResponse{DeleteFileResponse: deleteFileResponse}}
				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_CommandResponse{CommandResponse: responseWrapper}}
				reqHandler.Send(wrapper)

				break parentSwitch
			}
		case *proto.Wrapper_ComputeCommands:
			s.logger.Info("Got a compute command request")
			s.handleComputeCommands(reqHandler, request.GetComputeCommands())
		}
	}
}

func (s *Server) handleComputeCommands(reqHandler *proto.MessageHandler, request *proto.ComputeCommands) {
	switch request.GetCommands().(type) {
	case *proto.ComputeCommands_FileInfoRequest:
		s.logger.Info("Got request to get file info")
		getFileInfoResponse := &proto.FileInfoResponse{}
		targetExist := s.fs.CheckDirExist(request.GetFileInfoRequest().GetDestPath())

		if !targetExist {
			s.logger.Error("Invalid destination")
			getFileInfoResponse.ErrorMessage = "Invalid destination"
			responseWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_FileInfoResponse{FileInfoResponse: getFileInfoResponse}}
			wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: responseWrapper}}
			reqHandler.Send(wrapper)
			return
		}

		s.logger.Info("Checking for file id")
		fileId, fileSize, errorMessage := s.fs.CheckIsFile(request.GetFileInfoRequest().GetFilePath())

		if errorMessage != "" {
			s.logger.Error(errorMessage)
			getFileInfoResponse.ErrorMessage = errorMessage
			responseWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_FileInfoResponse{FileInfoResponse: getFileInfoResponse}}
			wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: responseWrapper}}
			reqHandler.Send(wrapper)
			return
		}

		s.logger.Info("Getting chunk addr")
		// map[chunkNum][]nodeIps
		chunkAddr, ok := s.fs.GetFileChunkAddresses(uint64(fileId))

		if !ok {
			s.logger.Error("Error in getting chunk addresses")
			getFileInfoResponse.ErrorMessage = "Error in getting chunk addresses"
			responseWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_FileInfoResponse{FileInfoResponse: getFileInfoResponse}}
			wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: responseWrapper}}
			reqHandler.Send(wrapper)
			return
		}

		s.logger.Info("Getting inverted chunk db count")
		// map[nodeIp][]chunkNames
		invertedChunkAddr := invertDbChunkCount(chunkAddr)
		chunksPerIpList := make([]*proto.ChunksPerIp, 0)
		for nodeIp, chunkNameList := range invertedChunkAddr {
			chunkPerIp := &proto.ChunksPerIp{
				NodeIp:    nodeIp,
				ChunkName: chunkNameList,
			}

			chunksPerIpList = append(chunksPerIpList, chunkPerIp)
		}

		getFileInfoResponse.ChunksPerIp = chunksPerIpList
		getFileInfoResponse.FileSize = fileSize
		responseWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_FileInfoResponse{FileInfoResponse: getFileInfoResponse}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: responseWrapper}}

		s.logger.Info("Sending response for getFileInfo")

		reqHandler.Send(wrapper)
	}
}

func invertDbChunkCount(chunkAddr map[uint64]*filesystem.DbChunkCount) map[string][]string {
	invertedMap := make(map[string][]string)

	for _, chunkInfo := range chunkAddr {
		ipList := chunkInfo.NodeIps
		chunkName := chunkInfo.ChunkName

		for _, ip := range ipList {
			if _, present := invertedMap[ip]; !present {
				invertedMap[ip] = make([]string, 0)
			}

			invertedMap[ip] = append(invertedMap[ip], chunkName)
		}
	}

	return invertedMap
}

/** Since before we listen for new information, we have to first register the node, so we need to pass in registrationMessage */
func (s *Server) handleNode(reqHandler *proto.MessageHandler, registrationMessage *proto.Wrapper) {
	s.logger.Info("Handling new storage node")

	// returning pointer to node here to minimize interaction with map
	node, nodeId, ok := s.registerNode(registrationMessage, reqHandler)

	if !ok {
		s.logger.Error("Error registering node")
		return
	}

	for {
		receivedMessage, err := reqHandler.Receive()

		if err != nil {
			s.logger.Error("Error in receiving message from node/client (message next line)")
			s.logger.Error(err.Error())
			continue
		}

		if time.Since(node.LastHeartBeat).Seconds() > 30 {
			// keeping a track of disconnected nodes as well, idk why
			node.MarkOffline() // this just updates the db
			node.Active = false
			break
		}

		switch receivedMessage.Messages.(type) {
		case *proto.Wrapper_HeartBeat:
			received := receivedMessage.GetHeartBeat()
			if received.GetId() != nodeId {
				//s.logger.Error("Received id from node and id we have stored is not the same")
				return
			}
			// just for logging purposes
			//lastHeartBeat := node.LastHeartBeat
			//s.logger.Infof("Got heartbeat from storage node %s and since %f", nodeId, time.Since(lastHeartBeat).Seconds())
			node.LastHeartBeat = time.Now()

			usedSpace := node.UsedSpace
			if usedSpace != node.TotalSize-received.UsedSpace { // this is actually received.AvailableSpace, don't wanna mess up with proto file right now
				node.UsedSpace = node.TotalSize - received.UsedSpace
				//node.UpdateUsedSpace() // Not updating UsedSpace right now since all nodes are on one machine anyway
			}

			if node.TotalRequest != received.TotalRequest {
				node.TotalRequest = received.TotalRequest
				//node.UpdateTotalRequest()
			}

		case *proto.Wrapper_ChunkedReceivedAck:

			s.logger.Debugf("Got chunkreceived ack for fileId %d", receivedMessage.GetChunkedReceivedAck().GetFileId())

			fileId := receivedMessage.GetChunkedReceivedAck().GetFileId()
			chunkNumber := receivedMessage.GetChunkedReceivedAck().GetChunkNumber()
			chunkName := receivedMessage.GetChunkedReceivedAck().GetChunkName()
			chunkSize := receivedMessage.GetChunkedReceivedAck().GetChunkSize()
			startingOffset := receivedMessage.GetChunkedReceivedAck().GetStartingOffset()

			if receivedMessage.GetChunkedReceivedAck().GetErrorMessage() != "" {
				s.logger.Errorf("Node %s did not receive chunk num %d chunkName %s", node.Id, chunkNumber, chunkName)
				break
			}

			s.fs.InsertChunk(fileId, chunkNumber, chunkName, node.ConnectionAddress, chunkSize, startingOffset)

			if status, err := s.fs.GetFileStatus(fileId); err != false && status != filesystem.Ready {
				expectedFileChunkCount, _ := s.fs.ExpectedChunkCount(fileId)
				actualFileChunkCount, _ := s.fs.GetFileChunkAddresses(fileId)
				if expectedFileChunkCount == uint64(len(actualFileChunkCount)) {
					s.fs.UpdateFileStatus(fileId, "READY")
				}
			}

			// if client does not want to replicate we break
			if receivedMessage.GetChunkedReceivedAck().GetReplication() != true {
				break
			}

			chunkInfo, infoOk := s.fs.GetCountOfChunk(chunkName)

			if !infoOk {
				s.logger.Error("Error in getting chunk info")
				panic("Error")
			}

			chunkCount := len(chunkInfo.AvailableIn)

			if chunkCount < 3 && (len(chunkInfo.NotIn) > chunkCount) {
				unluckyNode := chunkInfo.AvailableIn[rand.Intn(len(chunkInfo.AvailableIn))]
				nodeConnection, _ := net.Dial("tcp", unluckyNode)
				nodeConnectionHandler := proto.NewMessageHandler(nodeConnection)

				for key, present := range chunkInfo.NotIn {
					if present == true { // if notIn == true
						sendReplicaRequest := &proto.SendReplicaRequest{
							ChunkName:      chunkInfo.ChunkName,
							TargetIp:       key,
							FileId:         fileId,
							ChunkNumber:    chunkInfo.ChunkNumber,
							ChunkSize:      chunkInfo.ChunkSize,
							StartingOffset: chunkInfo.StartingOffset,
							Replication:    receivedMessage.GetChunkedReceivedAck().GetReplication(),
						}

						wrapper := &proto.Wrapper{Messages: &proto.Wrapper_SendReplicaRequest{SendReplicaRequest: sendReplicaRequest}}
						nodeConnectionHandler.Send(wrapper)
						break // ensures that we only send to one node at a time
					}
				}
			}

			break
		}
	}
}

/**
Method takes care of registering the node and getting the node id
*/
func (s *Server) registerNode(registrationMessage *proto.Wrapper, reqHandler *proto.MessageHandler) (*Node, string, bool) {
	received := registrationMessage.GetRegistration()
	// initially there will be no used space in server
	node, ok := InitNode(received.GetConnectionAddress(), received.GetTotalSize(), 0, s.logger, s.db, s.dbLock)

	if !ok {
		ack := &proto.RegistrationAck{
			Accepted: false,
		}
		s.logger.Error("Unable to initialize storage nodes, see previous logs")
		reqHandler.Send(&proto.Wrapper{Messages: &proto.Wrapper_RegistrationAck{RegistrationAck: ack}})
		return nil, "", false
	}

	nodeId := node.Id

	s.nodeMap[nodeId] = node
	//s.nodeMapOperationChannel <- &NodeMapOperation{
	//	Operation: Put,
	//	Key:       nodeId,
	//	Put: &PutStruct{
	//		StructField: NodePointer,
	//		Value:       &MapValue{NodePointer: node},
	//	},
	//}

	ack := &proto.RegistrationAck{
		Accepted: true,
		Id:       nodeId,
		Message:  "Success",
	}

	reqHandler.Send(&proto.Wrapper{Messages: &proto.Wrapper_RegistrationAck{RegistrationAck: ack}})

	return node, nodeId, true
}

func (s *Server) sendWrapper() {
	//for {
	//	wrapper := <-n.sendChannel
	//	n.serverHandler.Send(wrapper)
	//}
}

func parseArgs(args []string, logger *log.Logger) (string, string, bool) {
	logger.Info("Parsing command line arguments")

	var (
		listenerAddr string
		dbPath       string
		freshTable   bool
	)

	for i := 0; i < len(args); i++ {
		if listenerAddr == "" && (args[i] == "-la" || args[i] == "-listenAddr") {
			listenerAddr = args[i+1]
			i++
		} else if dbPath == "" && args[i] == "-db" {
			dbPath = args[i+1]
			i++
		} else if args[i] == "-freshTable" {
			freshTable, _ = strconv.ParseBool(args[i+1])
			i++
		}
	}

	logger.Infof("Got la as %s, dbPath as %s and freshTable as %d", listenerAddr, dbPath, freshTable)

	return listenerAddr, dbPath, freshTable
}

func (s *Server) nodeMapOperations() {
	for {
		operation := <-s.nodeMapOperationChannel
		key := operation.Key

	parentSwitch:
		switch operation.Operation {
		case Get:

			switch operation.Get.Request.StructField {
			case LastHeartBeat:
				s.nodeMapOperationChannel <- &NodeMapOperation{
					Operation: operation.Operation,
					Key:       key,
					Get: &GetStruct{
						Response: &GetResponse{
							StructField: LastHeartBeat,
							Value: &MapValue{
								Time: s.nodeMap[key].LastHeartBeat,
							},
						},
					},
				}
				break parentSwitch
			case IsActive:
				s.nodeMapOperationChannel <- &NodeMapOperation{
					Operation: operation.Operation,
					Key:       key,
					Get: &GetStruct{
						Response: &GetResponse{
							StructField: IsActive,
							Value: &MapValue{
								Bool: s.nodeMap[key].Active,
							},
						},
					},
				}
				break parentSwitch
			case UsedSpace:
				s.nodeMapOperationChannel <- &NodeMapOperation{
					Operation: operation.Operation,
					Key:       key,
					Get: &GetStruct{
						Response: &GetResponse{
							StructField: UsedSpace,
							Value: &MapValue{
								Uint64: s.nodeMap[key].UsedSpace,
							},
						},
					},
				}
				break parentSwitch
			case NodePointer:
				s.nodeMapOperationChannel <- &NodeMapOperation{
					Operation: operation.Operation,
					Key:       key,
					Get: &GetStruct{
						Response: &GetResponse{
							StructField: NodePointer,
							Value: &MapValue{
								NodePointer: s.nodeMap[key],
							},
						},
					},
				}
				break parentSwitch
			}
			break parentSwitch
		case GetResult:
			s.nodeMapOperationChannel <- operation
			break
		case Put:
			switch operation.Put.StructField {
			case NodePointer:
				val := operation.Put.Value.NodePointer
				s.nodeMap[key] = val
				break parentSwitch
			case LastHeartBeat:
				val := operation.Put.Value.Time
				s.nodeMap[key].LastHeartBeat = val
				break parentSwitch
			case UsedSpace:
				val := operation.Put.Value.Uint64
				s.nodeMap[key].UsedSpace = val
				break parentSwitch
			case IsActive:
				val := operation.Put.Value.Bool
				s.nodeMap[key].Active = val
				break parentSwitch
			}
			break parentSwitch
		}
	}
}

func (s *Server) handleReplication() {
	var entriesPresent bool
	for {
		entriesPresent = s.fs.CheckIfTableEmpty()
		if !entriesPresent {
			time.Sleep(time.Second * 60)
			continue
		}

		s.logger.Debugf("Entries present in chunk table, checking whether any replication is required or not")

		fileInfo, _ := s.fs.GetAllChunksInfo()

		/**
		For each file
			for each chunk
		*/
		rand.Seed(time.Now().Unix()) // initialize global pseudo random generator
		for fileId, fInfo := range fileInfo {
			for _, chunkInfo := range fInfo.ChunkInfo {
				if len(chunkInfo.AvailableIn) >= 3 || len(chunkInfo.NotIn) < 3 {
					// if enough replicas or not enough storage nodes
					continue
				}

				replicasToBeMade := MinimumReplicas - len(chunkInfo.AvailableIn)
				s.logger.Debugf("%s has %d replicas to be made", chunkInfo.ChunkName, replicasToBeMade)
				// node which will send replicas to other nodes
				unluckyNode := chunkInfo.AvailableIn[rand.Intn(len(chunkInfo.AvailableIn))]

				targetIpList := make([]string, 0)
				for targetIps, _ := range chunkInfo.NotIn {
					if replicasToBeMade == 0 {
						break
					}
					targetIpList = append(targetIpList, targetIps)
					replicasToBeMade--
				}

				nodeConnection, _ := net.Dial("tcp", unluckyNode)
				nodeConnectionHandler := proto.NewMessageHandler(nodeConnection)

				sendReplicaRequest := &proto.SendReplicaRequest{
					ChunkName:   chunkInfo.ChunkName,
					TargetIp:    targetIpList[0],
					FileId:      fileId,
					ChunkNumber: chunkInfo.ChunkNumber,
					ChunkSize:   chunkInfo.ChunkSize,
				}

				wrapper := &proto.Wrapper{Messages: &proto.Wrapper_SendReplicaRequest{SendReplicaRequest: sendReplicaRequest}}
				nodeConnectionHandler.Send(wrapper)

				s.logger.Debugf("Sent replica from %s for chunkedFile %s", unluckyNode, chunkInfo.ChunkName)
			}
		}

		time.Sleep(time.Second * 60)
	}
}
