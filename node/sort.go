package main

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func printKVPairs(keyValueList []*KeyValueStruct, currentMinKV *KeyValueStruct, logger *log.Logger) {

	if keyValueList != nil {
		for i, pair := range keyValueList {
			logger.Infof("Pair %d and key == %s\n", i, pair.key)
			for j, val := range pair.value {
				logger.Infof("Val %d -> %d\n", j, val)
			}
		}

	} else {
		logger.Infof("Current minkey is %s", currentMinKV.key)
		for j, val := range currentMinKV.value {
			logger.Infof("Val %d -> %d\n", j, val)
		}
	}

	logger.Info("---------------")
}

func StartSort(fileName string, bufSize int32, outputFileName string, logger *log.Logger) {
	s := InitSorter(bufSize, fileName, logger)
	conqueror := InitConqueror(bufSize, logger)

	logger.Info("Initialized sorter and conqueror")

	bufNum := 0
	for {
		out := s.fillBuffer()
		//logger.Infof("Filled buffer %d and buffer is", bufNum)
		//logger.Info(out)

		if len(out) == 0 {
			logger.Info("Scanned the entire file, exiting loop")
			break
		}

		keyValueList := getKeyValuePairs(out)
		//logger.Info("Got a list of KV pairs")
		//printKVPairs(keyValueList, nil, logger)

		sort.Slice(keyValueList, func(i, j int) bool { return keyValueList[i].key < keyValueList[j].key })
		//logger.Info("Sorted KV pairs")
		//printKVPairs(keyValueList, nil, logger)

		interFileName := strings.Split(fileName, ".")[0] + "sort_" + fmt.Sprint(bufNum) + ".txt"

		interFile, _ := os.Create(interFileName)

		for _, kv := range keyValueList {
			writeLine := kv.key
			for _, val := range kv.value {
				//logger.Infof("Writing line %s ---> %s to intermediate file %s\n", kv.key, fmt.Sprint(val), interFileName)
				writeLine += " " + fmt.Sprint(val)
			}
			interFile.WriteString(writeLine + "\n")
		}

		conqueror.AddFile(interFileName, logger)
		interFile.Close()

		//logger.Infof("Closed inter file %s", interFileName)
		bufNum++
	}

	conqueror.Conquer(outputFileName, logger)
}

type Sorter struct {
	bufferSize  int32
	fileName    string
	fileScanner *bufio.Scanner
	file        *os.File
}

type KeyValueStruct struct {
	key   string
	value []int
}

type Conqueror struct {
	maxBuffer         int32
	bufferSizePerFile int32
	sortedFile        *os.File
	sortedFileScanner *bufio.Scanner
	fileMap           map[string]*InterFileInfo
}

type InterFileInfo struct {
	file     *os.File
	scanner  *bufio.Scanner
	fileName string
	complete bool
	buffer   []string
}

func InitConqueror(maxBuffer int32, logger *log.Logger) *Conqueror {
	conqueror := &Conqueror{}
	conqueror.maxBuffer = maxBuffer
	conqueror.fileMap = make(map[string]*InterFileInfo)
	return conqueror
}

func (c *Conqueror) AddFile(fileName string, logger *log.Logger) {
	if _, present := c.fileMap[fileName]; present {
		panic("File with similar name already exists")
	}

	file, _ := os.Open(fileName)
	c.fileMap[fileName] = &InterFileInfo{
		file:     file,
		fileName: fileName,
		scanner:  bufio.NewScanner(file),
		complete: false,
		buffer:   make([]string, 0),
	}

	//logger.Infof("File %s has been added to the conqueror\n", fileName)
}

func (c *Conqueror) printBuffer(logger *log.Logger) {
	for _, fileInfo := range c.fileMap {
		logger.Infof("In file %s\n", fileInfo.fileName)
		for _, x := range fileInfo.buffer {
			logger.Infof("%s ", x)
		}
		logger.Info()
	}
}

func (c *Conqueror) Conquer(outputFileName string, logger *log.Logger) {
	bufferSizePerFile := int32(math.Max(1, math.Floor(float64(c.maxBuffer)/float64(len(c.fileMap)))))
	c.bufferSizePerFile = bufferSizePerFile

	logger.Infof("Starting conqueror, buffer size per file is %d", bufferSizePerFile)

	sortedFile, _ := os.Create(outputFileName)
	scanner := bufio.NewScanner(sortedFile)

	c.sortedFile = sortedFile
	c.sortedFileScanner = scanner

	c.fillFileBuffers()

	complete := false

	prevMinKV := &KeyValueStruct{}

	for {
		complete = c.checkComplete(logger)

		if complete {
			logger.Info("Completed")
			break
		}

		currentMinKV := c.getMinKV()
		//printKVPairs(nil, currentMinKV, logger)

		if currentMinKV.key == prevMinKV.key {
			prevMinKV.value = append(prevMinKV.value, currentMinKV.value...)
		} else if prevMinKV.key != "" {
			writeLine := prevMinKV.key
			for _, val := range prevMinKV.value {
				writeLine += " " + fmt.Sprint(val)
			}
			//logger.Infof("Writing line %s to out", writeLine)
			c.sortedFile.WriteString(writeLine + "\n")

			prevMinKV.key = currentMinKV.key
			prevMinKV.value = currentMinKV.value
		} else {
			prevMinKV.key = currentMinKV.key
			prevMinKV.value = currentMinKV.value
		}
		c.fillFileBuffers()
	}

	// almost done, we just have to now write the prevMinKey since by the time that gets grouped, complete will be true for all files, and
	// we will have exited the loop

	writeLine := prevMinKV.key
	for _, val := range prevMinKV.value {
		writeLine += " " + fmt.Sprint(val)
	}
	c.sortedFile.WriteString(writeLine + "\n")

	for _, fileInfo := range c.fileMap {
		fileInfo.file.Close()
	}

	//logger.Info("File is closed, content of buffers are the following")
	//c.printBuffer(logger)
	//logger.Info("Prev minKV are the following")
	//logger.Info(prevMinKV.key)
	//for _, val := range prevMinKV.value {
	//	logger.Infof("%d ", val)
	//}
	//logger.Info()

	c.sortedFile.Close()

	logger.Info("Sort complete")
}

func (c *Conqueror) getMinKV() *KeyValueStruct {
	currentMinKV := &KeyValueStruct{}
	minFileName := ""
	for _, fileInfo := range c.fileMap {
		if fileInfo.complete && len(fileInfo.buffer) == 0 {
			continue
		}
		if currentMinKV.key == "" {
			line := fileInfo.buffer[0]
			currentMinKV = c.getKV(line)
			minFileName = fileInfo.fileName
			continue
		}

		line := fileInfo.buffer[0]
		kv := c.getKV(line)

		if kv.key < currentMinKV.key {
			currentMinKV.key = kv.key
			currentMinKV.value = kv.value
			minFileName = fileInfo.fileName
		}
	}

	if len(c.fileMap[minFileName].buffer) > 1 {
		c.fileMap[minFileName].buffer = c.fileMap[minFileName].buffer[1:]
	} else {
		c.fileMap[minFileName].buffer = make([]string, 0)
	}
	return currentMinKV
}

func (c *Conqueror) checkComplete(logger *log.Logger) bool {
	for _, fileInfo := range c.fileMap {
		//logger.Infof("checking for whether is %s completed, complete = %d \n", fName, fileInfo.complete)
		if !fileInfo.complete || len(fileInfo.buffer) > 0 {
			return false
		}
	}

	return true
}

func (c *Conqueror) fillFileBuffers() {
	for _, fileInfo := range c.fileMap {
		if fileInfo.complete {
			continue
		}

		if len(fileInfo.buffer) == 0 {
			lineCount := int32(0)

			for {
				present := fileInfo.scanner.Scan()

				if !present {
					//logger.Infof("%s is completed after %d lines\n", fName, lineCount)
					fileInfo.complete = true
					break
				}

				line := fileInfo.scanner.Text()

				//if strings.Split(line, " ")[0] == "s" {
				//	logger.Info("Got s")
				//}
				fileInfo.buffer = append(fileInfo.buffer, line)
				lineCount++

				if lineCount == c.bufferSizePerFile {
					break
				}
			}
		}
	}
}

func (c *Conqueror) getKV(line string) *KeyValueStruct {

	currentMinKV := &KeyValueStruct{}

	splitLine := strings.Split(line, " ")
	key := splitLine[0]
	value := splitLine[1:]

	currentMinKV.key = key
	currentMinKV.value = make([]int, 0)

	for _, val := range value {
		intVal, _ := strconv.Atoi(val)
		currentMinKV.value = append(currentMinKV.value, intVal)
	}

	return currentMinKV

}

func getKeyValuePairs(buffer []string) []*KeyValueStruct {
	out := make(map[string][]int)
	for _, line := range buffer {
		splitLine := strings.Split(line, " ")
		key := splitLine[0]
		value := splitLine[1:]

		if _, present := out[key]; !present {
			out[key] = make([]int, 0)
		}

		for _, val := range value {
			intVal, _ := strconv.Atoi(val)
			out[key] = append(out[key], intVal)
		}
	}

	outList := make([]*KeyValueStruct, 0)

	for key, value := range out {
		kvStruct := &KeyValueStruct{
			key:   key,
			value: value,
		}

		outList = append(outList, kvStruct)
	}

	return outList
}

func InitSorter(bufferSize int32, fileName string, logger *log.Logger) *Sorter {

	sorter := &Sorter{}

	sorter.bufferSize = bufferSize
	sorter.fileName = fileName

	file, err := os.Open(fileName)

	if err != nil {
		panic("Error in opening the file" + err.Error())
	}

	scanner := bufio.NewScanner(file)

	sorter.file = file
	sorter.fileScanner = scanner

	return sorter
}

func (s *Sorter) fillBuffer() []string {
	out := make([]string, 0)
	lineCount := int32(0)

	for s.fileScanner.Scan() {
		out = append(out, s.fileScanner.Text())
		lineCount++
		if lineCount == s.bufferSize {
			break
		}
	}

	return out
}

func (s *Sorter) empty() {

}
