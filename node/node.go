package main

import (
	"Hackerman/proto/proto"
	"Hackerman/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"net"
	"os"
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
}

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

/**
This function only handles request made TO the node
Only 3 types of requests are supported -> client asks to download/upload and server asks to replicate
*/
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
				FileId:      request.GetSendReplicaRequest().GetFileId(),
				ChunkName:   chunkName,
				ChunkNumber: request.GetSendReplicaRequest().GetChunkNumber(),
				ChunkSize:   request.GetSendReplicaRequest().GetChunkSize(),
				ChunkedFile: fileBytes,
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

			//_, openErr := os.Stat(chunkName)
			//if openErr == nil {
			//	// if a file with the same name exists, we delete that file first
			//	// although the way WritFile seems to work is that it overwrites the file anyways
			//https://groups.google.com/g/golang-nuts/c/3poQukyO2QY?pli=1
			//	os.Remove(chunkName)
			//}

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
		}

	}
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
