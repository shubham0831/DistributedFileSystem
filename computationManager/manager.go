package main

import (
	"Hackerman/proto/proto"
	"Hackerman/utils"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
)

const ONE_GB = 1024 * 1024 * 1024

// MIN_REDUCER_CAPACITY
// if user has not provided number of reducers, we will have fileSize/MIN_REDUCER_CAPACITY number of reducers
const MIN_REDUCER_CAPACITY = ONE_GB * 1

const MAX_LOCALIZATION_PERCENTAGE = 0.3
const MIN_CHUNKS_PER_NODE = 2

type Connections struct {
	clientConn *proto.MessageHandler
	serverConn *proto.MessageHandler
}

type Manager struct {
	logger          *log.Logger
	listener        net.Listener
	responseChannel chan *ControllerMessage
}

func InitManager(listenAddr string, logger *log.Logger) *Manager {
	listener, listenErr := net.Listen("tcp", listenAddr)

	if listenErr != nil {
		logger.Panic("Error setting up listener")
	}

	manager := &Manager{
		logger:          logger,
		listener:        listener,
		responseChannel: make(chan *ControllerMessage, 100),
	}

	return manager
}

func main() {
	logger := utils.GetLogger(log.DebugLevel, os.Stdout)
	listenerAddr := parseArgs(os.Args, logger)

	manager := InitManager(listenerAddr, logger)
	manager.start()
}

func (m *Manager) start() {
	m.logger.Debugf("Starting listener loop")

	for {
		if conn, err := m.listener.Accept(); err == nil {
			reqHandler := proto.NewMessageHandler(conn)
			go m.handleRequest(reqHandler)
		} else {
			m.logger.Error(err.Error())
			m.logger.Error("Unable to accept request")
		}
	}
}

func (m *Manager) handleRequest(reqHandler *proto.MessageHandler) {
	for {
		request, err := reqHandler.Receive()
		if err != nil {
			m.logger.Error("Error in receiving message from node/client (message next line)")
			m.logger.Error(err.Error())
			continue
		}

		switch request.Messages.(type) {
		case *proto.Wrapper_ComputeJob:
			switch request.GetComputeJob().Commands.(type) {
			case *proto.ComputeJob_MapReduceRequest:
				m.logger.Info("Got a map reduce request")
				connection := &Connections{
					clientConn: reqHandler,
					serverConn: getConnection(request.GetComputeJob().GetMapReduceRequest().GetServerAddr(), m.logger),
				}

				mapReduceJob := InitJob(connection, request.GetComputeJob().GetMapReduceRequest(), m.logger)
				mapReduceJob.StartJob()
			}
		}
	}
}

func getConnection(address string, logger *log.Logger) *proto.MessageHandler {
	serverConn, serErr := net.Dial("tcp", address)
	if serErr != nil {
		logger.Panicf("Error establishing connection to %s", address)
	}

	logger.Infof("Established connection to %s", address)
	handler := proto.NewMessageHandler(serverConn)
	return handler
}

func parseArgs(args []string, logger *log.Logger) string {
	logger.Info("Parsing command line arguments")

	var listenerAddr string

	for i := 0; i < len(args); i++ {
		if listenerAddr == "" && (args[i] == "-la" || args[i] == "-listenAddr") {
			listenerAddr = args[i+1]
			i++
		}
	}

	logger.Infof("Got la as %s", listenerAddr)

	return listenerAddr
}
