package main

import (
	"Hackerman/proto/proto"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"math"
	"sort"
	"sync"
)

type JobStatus int64

const (
	ONGOING JobStatus = iota
	DONE
)

type Mapper struct {
	NodeIp            string
	ChunkStatus       map[string]bool // has this chunk been mapped?
	ShufflingComplete bool
}

// Reducer // Not using this right now, might need to use it in the future
type Reducer struct {
	NodeIp      string
	FileReduced bool // has the reducer done its job
}

type Job struct {
	JobId                    string
	ClientRequest            *proto.MapReduceRequest
	JobStatus                JobStatus
	Connections              *Connections
	StatusUpdates            []string // once this reaches a size of 5, we send all the status updates to the client
	Mappers                  map[string]*Mapper
	Reducers                 map[string]bool
	responseChannel          chan *ControllerMessage
	handleResponseChannel    chan *HandleResponseStruct
	mappingComplete          bool
	logger                   *log.Logger
	mapJobFailedErrorMessage string
	filesSentToReducer       map[string][]string
	jobComplete              bool
}

func InitJob(connections *Connections, request *proto.MapReduceRequest, logger *log.Logger) *Job {
	logger.Info("Initiating MapReduce Job")
	job := &Job{}
	job.JobId = uuid.New().String()
	job.ClientRequest = request
	job.JobStatus = ONGOING
	job.Connections = connections
	job.StatusUpdates = make([]string, 0)
	job.Mappers = make(map[string]*Mapper)
	job.Reducers = make(map[string]bool)
	job.responseChannel = make(chan *ControllerMessage, 100)
	job.handleResponseChannel = make(chan *HandleResponseStruct, 100)
	job.logger = logger
	job.mapJobFailedErrorMessage = ""
	job.mappingComplete = false
	job.filesSentToReducer = make(map[string][]string)
	job.jobComplete = false

	job.sendStatusUpdate("Initialized Map Reduce Job", false)

	return job
}

func (j *Job) sendStatusUpdate(message string, jobComplete bool) {
	j.logger.Debug("Sending status update")
	msg := &proto.MRStatusUpdate{
		Message:     message,
		JobComplete: jobComplete,
	}

	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_MRStatusUpdate{MRStatusUpdate: msg}}

	j.Connections.clientConn.Send(wrapper)
}

func (j *Job) StartJob() {

	j.logger.Info("Starting Map Reduce job")

	j.sendStatusUpdate("Starting Map Reduce Job", false)

	go j.listenFromServer() // check if it's better to close this connection

	fileInfoRequest := &proto.FileInfoRequest{FilePath: j.ClientRequest.GetFilePath(), DestPath: j.ClientRequest.GetDestPath()}
	fileInfoRequestWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_FileInfoRequest{FileInfoRequest: fileInfoRequest}}
	reqWrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: fileInfoRequestWrapper}}
	j.Connections.serverConn.Send(reqWrapper)

	fileInfo := j.waitAndGetResponse(FileInfoResponse).FileInfo
	j.sendStatusUpdate("Got file information from server", false)

	if fileInfo.ErrorMessage != "" {
		j.JobStatus = DONE
		j.sendStatusUpdate("Got following error in getting file info from server \n "+fileInfo.ErrorMessage, true)
	}

	j.assignJobs(fileInfo)
	j.startJob()
}

func (j *Job) startJob() {
	reducerList := make([]string, 0)

	for reducer, _ := range j.Reducers {
		reducerList = append(reducerList, reducer)
	}

	go j.handleResponseLoop()
	var wg sync.WaitGroup

	// some reducers might not be a mapper, we have to send them the plugin
	j.sendPluginToExcludedReducers(j.Mappers, j.Reducers)
	for mapper, mapInfo := range j.Mappers {
		wg.Add(1)
		go func(nodeIp string, mapInfo *Mapper, reducerList []string) {
			defer wg.Done()
			j.handleMapRequest(nodeIp, mapInfo, reducerList, int32(len(mapper)))
		}(mapper, mapInfo, reducerList)
	}

	wg.Wait()

	for {
		if j.mappingComplete {
			break
		}
	}

	j.sendStatusUpdate("Mapping phase is complete, starting reduce phase", false)

	j.logger.Info("Mapping phase is complete")

	var wg2 sync.WaitGroup

	for reducerIp, _ := range j.Reducers {
		wg2.Add(1)
		if _, present := j.filesSentToReducer[reducerIp]; present {
			go func(reducerIp string) {
				defer wg2.Done()
				j.handleReduceRequest(reducerIp, j.filesSentToReducer)
			}(reducerIp)
		}
	}

	wg2.Wait()

	for {
		if j.jobComplete {
			break
		}
	}

	j.sendStatusUpdate("Job is completed part 2", true)
	j.logger.Info("Map reduce job is complete")
}

func (j *Job) handleReduceRequest(reducerIp string, allReducersFileList map[string][]string) {
	nodeHandler := getConnection(reducerIp, j.logger)
	go j.listenFromNode(nodeHandler)

	reducerFileMap := make(map[string]*proto.ReduceFileList)
	for reducersIp, fileNames := range allReducersFileList {
		reducerFileMap[reducersIp] = &proto.ReduceFileList{FileNames: fileNames}
	}

	reduceRequest := &proto.ReduceRequest{
		JobId:          j.JobId,
		ReducerFileMap: reducerFileMap,
		TargetPath:     j.ClientRequest.GetDestPath(),
	}

	requestWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobRequest{RunJobRequest: &proto.RunJobRequest{Request: &proto.RunJobRequest_ReduceRequest{ReduceRequest: reduceRequest}}}}
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: requestWrapper}}
	nodeHandler.Send(wrapper)
	j.sendStatusUpdate("Sending "+reducerIp+" request to reduce the file", false)
	for {

		if j.mapJobFailedErrorMessage != "" {
			break
		}

		mapResponse := j.waitAndGetResponse(ReduceResponse).ReduceResponse
		j.handleResponseChannel <- &HandleResponseStruct{
			NodeIp:               mapResponse.ReducerIp,
			ReduceResponseStruct: mapResponse,
			StopLoop:             false,
		}

		j.sendStatusUpdate("Sending "+reducerIp+" request to reduce the file", false)

		break
	}

}

func (j *Job) sendPluginToExcludedReducers(mappers map[string]*Mapper, reducers map[string]bool) {

	for reducer, _ := range reducers {
		if _, present := mappers[reducer]; present {
			continue
		}

		sendPluginRequest := &proto.PluginDownload{
			JobId:      j.JobId,
			PluginName: j.ClientRequest.GetPluginName(),
			Plugin:     j.ClientRequest.GetPlugin(),
		}

		requestWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobRequest{RunJobRequest: &proto.RunJobRequest{Request: &proto.RunJobRequest_PluginDownload{PluginDownload: sendPluginRequest}}}}
		wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: requestWrapper}}

		nodeHandler := getConnection(reducer, j.logger)
		nodeHandler.Send(wrapper)
		j.logger.Debugf("Sent a plugin to reducer (only) %s", reducer)
		nodeHandler.Close()
	}

	return
}

func (j *Job) handleMapRequest(nodeIp string, info *Mapper, reducers []string, numberOfMappers int32) {
	nodeHandler := getConnection(nodeIp, j.logger)
	go j.listenFromNode(nodeHandler)

	chunkList := make([]string, 0)
	for chunkName, _ := range info.ChunkStatus {
		chunkList = append(chunkList, chunkName)
	}

	mapReduceRequest := &proto.MapRequest{
		JobId:           j.JobId,
		ChunkNames:      chunkList,
		Reducers:        reducers,
		PluginName:      j.ClientRequest.GetPluginName(),
		Plugin:          j.ClientRequest.GetPlugin(),
		NumberOfMappers: numberOfMappers,
	}

	requestWrapper := &proto.ComputeCommands{Commands: &proto.ComputeCommands_RunJobRequest{RunJobRequest: &proto.RunJobRequest{Request: &proto.RunJobRequest_MapRequest{MapRequest: mapReduceRequest}}}}
	wrapper := &proto.Wrapper{Messages: &proto.Wrapper_ComputeCommands{ComputeCommands: requestWrapper}}
	nodeHandler.Send(wrapper)

	j.sendStatusUpdate("Sent "+nodeIp+" a request to map the file", false)
	// listening to response

	for {

		if j.mapJobFailedErrorMessage != "" {
			break
		}

		mapResponse := j.waitAndGetResponse(MapResponse).MapResponse
		j.handleResponseChannel <- &HandleResponseStruct{
			NodeIp:            nodeIp,
			MapResponseStruct: mapResponse,
			StopLoop:          false,
		}

		if mapResponse.ShufflingComplete {
			j.sendStatusUpdate(nodeIp+" has completed shuffling", false)
			break
		}
	}
}

// loop closes after mapping is done, so we are not able to handle reduce response
func (j *Job) handleResponseLoop() {
	for {
		updateMappersMapStruct := <-j.handleResponseChannel

		var stopLoop bool
		if updateMappersMapStruct.MapResponseStruct != nil {
			//stopLoop = j.handleMapResponse(updateMappersMapStruct)
			j.handleMapResponse(updateMappersMapStruct) // only want to stop the loop once reducers are done
		} else if updateMappersMapStruct.ReduceResponseStruct != nil {
			stopLoop = j.handleReduceResponse(updateMappersMapStruct)
		}

		if stopLoop {
			j.sendStatusUpdate("Job is completed", true)
			break
		}
	}
}

func (j *Job) handleReduceResponse(updateReduceStatus *HandleResponseStruct) bool {
	if updateReduceStatus.ReduceResponseStruct.ErrorMessage != "" {
		return true
	}

	j.Reducers[updateReduceStatus.ReduceResponseStruct.ReducerIp] = true
	j.logger.Infof("Reducer %s has the reduce output stored in file %s", updateReduceStatus.ReduceResponseStruct.ReducerIp, updateReduceStatus.ReduceResponseStruct.ReducedFileName)

	j.sendStatusUpdate("Reducer "+updateReduceStatus.ReduceResponseStruct.ReducerIp+" has completed its job and the output is stored in the file "+updateReduceStatus.ReduceResponseStruct.ReducedFileName, false)

	for _, completed := range j.Reducers {
		if !completed {
			return false
		}
	}

	j.jobComplete = true
	j.logger.Info("All reducers have completed their task")
	return true
}

func (j *Job) handleMapResponse(updateMappersMapStruct *HandleResponseStruct) bool {
	if updateMappersMapStruct.StopLoop == true {
		return true
	}

	nodeIp := updateMappersMapStruct.NodeIp
	mapResponse := updateMappersMapStruct.MapResponseStruct

	if mapResponse.ErrorMessage != "" { // send user update
		j.mapJobFailedErrorMessage = mapResponse.ErrorMessage
		return true
	}

	if mapResponse.ShufflingComplete {
		for reducerIp, fileSent := range mapResponse.FilesSentToReducer {
			if _, present := j.filesSentToReducer[reducerIp]; !present {
				j.filesSentToReducer[reducerIp] = make([]string, 0)
			}

			j.filesSentToReducer[reducerIp] = append(j.filesSentToReducer[reducerIp], fileSent)
		}
	}

	j.Mappers[nodeIp].ChunkStatus[mapResponse.ChunkName] = true
	j.Mappers[nodeIp].ShufflingComplete = mapResponse.ShufflingComplete

	if j.Mappers[nodeIp].ShufflingComplete {
		j.logger.Info("Got a shuffling is complete message from the node")
	}

	mappingComplete := j.checkIfMappingComplete()

	if mappingComplete {
		j.mappingComplete = mappingComplete
		return true
	}
	return false
}

func (j *Job) checkIfMappingComplete() bool {
	for _, chunkStatus := range j.Mappers {
		if !chunkStatus.ShufflingComplete {
			j.logger.Infof("Shuffling is not complete for node %s", chunkStatus.NodeIp)
			return false
		}
	}
	//j.logger.Infof("Shuffling is not complete for node %s", chunkStatus.NodeIp)
	return true
}

func (j *Job) listenFromNode(nodeHandler *proto.MessageHandler) {
	j.logger.Info("Listening for messages from node")

	for {
		response, err := nodeHandler.Receive()

		if err != nil {
			j.logger.Error("Error in receiving message from server (msg below) ")
			j.logger.Error(err.Error())
		}

		switch response.Messages.(type) {
		case *proto.Wrapper_ComputeCommands:
			switch response.GetComputeCommands().Commands.(type) {
			case *proto.ComputeCommands_RunJobResponse:
				switch response.GetComputeCommands().GetRunJobResponse().Response.(type) {
				case *proto.RunJobResponse_MapResponse:

					mapResponse := &MapResponseStruct{
						ChunkName:         response.GetComputeCommands().GetRunJobResponse().GetMapResponse().GetChunkName(),
						ShufflingComplete: response.GetComputeCommands().GetRunJobResponse().GetMapResponse().GetShufflingComplete(),
						ErrorMessage:      response.GetComputeCommands().GetRunJobResponse().GetMapResponse().GetErrorMessage(),
					}

					reducerFiles := response.GetComputeCommands().GetRunJobResponse().GetMapResponse().GetFilesSentToReducer()
					if reducerFiles == nil {
						j.responseChannel <- &ControllerMessage{
							ResponseCommand: MapResponse,
							MapResponse:     mapResponse,
						}
					}

					filesSentToReducer := make(map[string]string)
					for _, msg := range reducerFiles {
						filesSentToReducer[msg.GetReducerIp()] = msg.GetFileName()
					}

					mapResponse.FilesSentToReducer = filesSentToReducer
					j.responseChannel <- &ControllerMessage{
						ResponseCommand: MapResponse,
						MapResponse:     mapResponse,
					}

				case *proto.RunJobResponse_ReduceResponse:
					reduceResponse := &ReduceResponseStruct{
						ReducerIp:       response.GetComputeCommands().GetRunJobResponse().GetReduceResponse().GetNodeIp(),
						ReducedFileName: response.GetComputeCommands().GetRunJobResponse().GetReduceResponse().GetFileName(),
						ErrorMessage:    response.GetComputeCommands().GetRunJobResponse().GetReduceResponse().GetErrorMessage(),
					}
					j.logger.Infof("Got a reduce complete response from node %s", reduceResponse.ReducerIp)
					j.responseChannel <- &ControllerMessage{
						ResponseCommand: ReduceResponse,
						ReduceResponse:  reduceResponse,
					}
				}
			}
		}
	}
}

func (j *Job) listenFromServer() {
	j.logger.Info("Listening for messages from server")

	for {
		response, err := j.Connections.serverConn.Receive()

		if err != nil {
			j.logger.Error("Error in receiving message from server (msg below) ")
			j.logger.Error(err.Error())
		}

		switch response.Messages.(type) {
		case *proto.Wrapper_ComputeCommands:
			switch response.GetComputeCommands().Commands.(type) {
			case *proto.ComputeCommands_FileInfoResponse:
				//logger.Info("Got response back from server, response is")
				//logger.Info(response.GetComputeCommands().GetFileInfoResponse())
				fileInfo := &FileInfo{
					ErrorMessage: response.GetComputeCommands().GetFileInfoResponse().GetErrorMessage(),
					FileSize:     response.GetComputeCommands().GetFileInfoResponse().GetFileSize(),
				}

				chunksPerIpResponse := response.GetComputeCommands().GetFileInfoResponse().GetChunksPerIp()
				chunksPerIpMap := make(map[string][]string)

				for _, chunksPerIp := range chunksPerIpResponse {
					if _, present := chunksPerIpMap[chunksPerIp.GetNodeIp()]; !present {
						chunksPerIpMap[chunksPerIp.GetNodeIp()] = make([]string, 0)
					}

					chunksPerIpMap[chunksPerIp.GetNodeIp()] = append(chunksPerIpMap[chunksPerIp.GetNodeIp()], chunksPerIp.GetChunkName()...)
				}

				fileInfo.ChunksPerNode = chunksPerIpMap
				fileInfo.NodePerChunk = inverseMap(chunksPerIpMap)

				j.responseChannel <- &ControllerMessage{
					ResponseCommand: FileInfoResponse,
					FileInfo:        fileInfo,
				}
			}
		}
	}
}

func (j *Job) assignJobs(fileInfo *FileInfo) {

	numberOfMappers := j.ClientRequest.GetNumOfMappers()
	numberOfReducers := j.ClientRequest.GetNumOfReducers()

	j.logger.Info("Getting job assignments")
	j.sendStatusUpdate("Starting to assign jobs to nodes, there are "+fmt.Sprint(numberOfMappers)+" Mappers and "+fmt.Sprint(numberOfReducers)+" Reducers", false)

	if numberOfMappers == 0 {
		numberOfMappers = int32(len(fileInfo.ChunksPerNode)) // by default all chunks will be assigned map duties to spread out the load
	}

	if numberOfReducers == 0 {
		numberOfReducers = int32(math.Ceil(float64(fileInfo.FileSize) / float64(MIN_REDUCER_CAPACITY))) // each reducer
	}

	var jobAssignmentStruct *JobAssignmentStruct

	if j.ClientRequest.GetMaxParallelization() == true {
		jobAssignmentStruct = j.getMaxParallelizationAssignment(fileInfo, numberOfMappers, numberOfReducers)
	} else if j.ClientRequest.GetMaxParallelization() == false { // didn't need to specify == false, did it for readability
		jobAssignmentStruct = j.getMaxLocalizationAssignment(fileInfo, numberOfMappers, numberOfReducers)
	}

	// update the mappers map in the job struct

	m := jobAssignmentStruct.Mappers
	r := jobAssignmentStruct.Reducers

	for k, _ := range r {
		j.logger.Infof("Node %s is a reducer", k)
	}

	for k, v := range m {
		list := ""
		for k2, _ := range v {
			list += k2 + " "
		}
		j.logger.Infof("Total chunks assigned to %s is %d and has been assigned the following chunks \n %s \n  --------------- ", k, len(v), list)
	}

	j.populateMappers(jobAssignmentStruct.Mappers)
	j.populateReducers(jobAssignmentStruct.Reducers)

	j.sendStatusUpdate("Mappers and Reducers have been assigned", false)
}

func (j *Job) populateMappers(mappers map[string]map[string]bool) {
	for nodeIp, chunkSet := range mappers {
		mapper := &Mapper{}
		mapper.NodeIp = nodeIp
		mapper.ChunkStatus = make(map[string]bool)

		for chunks, _ := range chunkSet {
			mapper.ChunkStatus[chunks] = false
		}

		j.Mappers[nodeIp] = mapper
	}
}

func (j *Job) populateReducers(reducers map[string]bool) {
	for nodeIp, _ := range reducers {
		j.Reducers[nodeIp] = false
	}
}

func (j *Job) getMaxParallelizationAssignment(fileInfo *FileInfo, numberOfMappers int32, numberOfReducers int32) *JobAssignmentStruct {
	j.logger.Infof("Getting max parallelization assignment, number of chunks are %d", len(fileInfo.NodePerChunk))
	jobAssignmentStruct := &JobAssignmentStruct{}

	jobAssignmentStruct.Mappers = make(map[string]map[string]bool) // just made it a set
	jobAssignmentStruct.Reducers = make(map[string]bool)

	assignedChunks := make(map[string]bool)

	for chunkName, _ := range fileInfo.NodePerChunk {
		assignedChunks[chunkName] = false
	}

	// assigning mappers first
	for chunkName, nodeList := range fileInfo.NodePerChunk {

		if assignedChunks[chunkName] {
			continue
		}

		// first we assign chunk to a node which has not been assigned any chunk
		for _, node := range nodeList {
			if _, present := jobAssignmentStruct.Mappers[node]; !present {
				jobAssignmentStruct.Mappers[node] = make(map[string]bool)
				jobAssignmentStruct.Mappers[node][chunkName] = true
				assignedChunks[chunkName] = true
				break
			}
		}

		if assignedChunks[chunkName] == true {
			continue
		}

		// if all the available nodes have been assigned a chunk, then the chunk is not yet assigned
		// so we pick the node which has been assigned the minimum number of jobs
		minAssigned := math.MaxInt32
		minNodeIp := ""

		for _, node := range nodeList {
			if len(jobAssignmentStruct.Mappers[node]) < minAssigned {
				minAssigned = len(jobAssignmentStruct.Mappers[node])
				minNodeIp = node
			}
		}

		if _, present := jobAssignmentStruct.Mappers[minNodeIp]; !present {
			jobAssignmentStruct.Mappers[minNodeIp] = make(map[string]bool)
		}

		jobAssignmentStruct.Mappers[minNodeIp][chunkName] = true
		assignedChunks[chunkName] = true // chunk has been assigned
	}

	j.logger.Debug("Jobs have been assigned to mappers, starting reducer assignments")

	// assigning reducers
	// key = number of count in mapper
	// value is list of nodes with that count
	mapAssignmentCount := make(map[int32][]string)

	for key, value := range jobAssignmentStruct.Mappers {
		if _, present := mapAssignmentCount[int32(len(value))]; !present {
			mapAssignmentCount[int32(len(value))] = make([]string, 0)
		}

		mapAssignmentCount[int32(len(value))] = append(mapAssignmentCount[int32(len(value))], key)
	}

	maxJobsPerReducers := make([]int32, 0)

	// so if we have a map like the following
	/**
	map[10] = [nodeA, nodeB, nodeC]
	map[9] = [nodeD, nodeE, nodeF]
	...
	our output will be a list of the following values
	[10, 10, 10, 9, 9, 9 .....]

	we then sort this list, iterate over it backwards until we get the number of reducers we need
	don't need to iterate backwards, we have a custom sort function defined
	*/
	for key, _ := range mapAssignmentCount {
		maxJobsPerReducers = append(maxJobsPerReducers, key)
	}

	//https://forum.golangbridge.org/t/how-to-sort-int32/10529
	//https://go.dev/play/p/9s_noUV5sDA
	sort.Slice(maxJobsPerReducers, func(i, j int) bool { return maxJobsPerReducers[j] < maxJobsPerReducers[i] })

	for i := 0; i < len(maxJobsPerReducers); i++ {
		if int32(len(jobAssignmentStruct.Reducers)) == numberOfReducers {
			break
		}

		for _, val := range mapAssignmentCount[maxJobsPerReducers[i]] {
			jobAssignmentStruct.Reducers[val] = true

			if int32(len(jobAssignmentStruct.Reducers)) == numberOfReducers {
				break
			}
		}
	}

	j.logger.Debug("Max parallelization done")
	return jobAssignmentStruct
}

func (j *Job) getMaxLocalizationAssignment(fileInfo *FileInfo, numberOfMappers int32, numberOfReducers int32) *JobAssignmentStruct {
	j.logger.Infof("Getting max localization assignment, number of chunks are %d", len(fileInfo.NodePerChunk))
	jobAssignmentStruct := &JobAssignmentStruct{}
	numberOfChunks := int32(len(fileInfo.NodePerChunk))

	// node ip -> chunkNameSet
	jobAssignmentStruct.Mappers = make(map[string]map[string]bool)
	// reducerSet
	jobAssignmentStruct.Reducers = make(map[string]bool)

	minChunksInReducer := int32(math.Ceil(MAX_LOCALIZATION_PERCENTAGE * float64(numberOfChunks)))

	assignedChunks := make(map[string]bool)

	for chunkName, _ := range fileInfo.NodePerChunk {
		assignedChunks[chunkName] = false
	}

	// assigning the reducers
	for node, chunkList := range fileInfo.ChunksPerNode {
		if int32(len(jobAssignmentStruct.Reducers)) == numberOfReducers {
			break
		}

		jobAssignmentStruct.Reducers[node] = true

		for _, chunkName := range chunkList {
			if assignedChunks[chunkName] {
				continue
			}

			if _, present := jobAssignmentStruct.Mappers[node]; !present {
				jobAssignmentStruct.Mappers[node] = make(map[string]bool)
			}

			if int32(len(jobAssignmentStruct.Mappers[node])) == minChunksInReducer {
				break
			}

			jobAssignmentStruct.Mappers[node][chunkName] = true
			assignedChunks[chunkName] = true
		}
	}

	j.logger.Debug("Jobs have been assigned to reducer, starting assignments for mapper")

	for chunkName, nodeList := range fileInfo.NodePerChunk {
		if assignedChunks[chunkName] {
			continue
		}

		for _, node := range nodeList {
			if _, present := jobAssignmentStruct.Mappers[node]; !present {
				jobAssignmentStruct.Mappers[node] = make(map[string]bool)
			}
			jobAssignmentStruct.Mappers[node][chunkName] = true
			assignedChunks[chunkName] = true
			break
		}
	}

	j.logger.Debug("Max localization assignments is done")

	return jobAssignmentStruct
}

func (j *Job) getChunkNames(fileInfo *FileInfo) map[string]bool {
	chunkMap := make(map[string]bool)

	for _, chunkList := range fileInfo.ChunksPerNode {
		for _, chunkName := range chunkList {
			if _, present := chunkMap[chunkName]; !present {
				chunkMap[chunkName] = true
			}
		}
	}

	return chunkMap
}

func inverseMap(givenMap map[string][]string) map[string][]string {
	inverse := make(map[string]map[string]bool)

	// key = nodeIp
	// value = list of chunks
	for key, valueList := range givenMap {
		// value is the individual chunk
		for _, value := range valueList {
			if _, present := inverse[value]; !present {
				inverse[value] = make(map[string]bool)
			}
			inverse[value][key] = true
		}
	}

	out := make(map[string][]string)

	for key, value := range inverse {
		if _, present := out[key]; !present {
			out[key] = make([]string, 0)
		}

		for valKey, _ := range value {
			out[key] = append(out[key], valKey)
		}
	}

	return out
}

/**
Method just waits for appropriate type of response to come and then returns that response
*/
func (j *Job) waitAndGetResponse(command ResponseCommand) *ControllerMessage {
	for {
		res := <-j.responseChannel
		if res.ResponseCommand != command {
			j.responseChannel <- res
			continue
		}
		return res
	}
}

func (j *Job) empty() {
	//j.logger.Info("List of files which each reducer should have")
	//
	//for reducerIp, fileInfo := range j.filesSentToReducer {
	//	j.logger.Infof("%s should have a total of %d files", reducerIp, len(fileInfo))
	//	for _, files := range fileInfo {
	//		j.logger.Info(files)
	//	}
	//	j.logger.Info("----------------------")
	//}
}
