package filesystem

import (
	"bufio"
	"bytes"
	"database/sql"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
	"path/filepath"
	"sync"
	"time"
)

type FileSystem struct {
	db     *sql.DB
	dbLock *sync.Mutex
	logger *logrus.Logger
}

type FileStatus int64

type NodeR struct {
	Id                string
	Active            bool
	ConnectionAddress string
	db                *sql.DB
	LastHeartBeat     time.Time
	TotalSize         uint64
	UsedSpace         uint64
	logger            *log.Logger
	dbLock            *sync.Mutex
	TotalRequest      int64
	/**
	Will be adding chunk info as well? Maybe just query db to get relevant ips
	*/
}

const (
	Ready FileStatus = iota
	InProgress
	Lost
)

type dbChunkCount struct {
	ChunkName string
	NodeIps   []string
}

// map[uint64]*FileInfo // id is fileId
/* FileInfo
ExpectedChunks uint64
map[uint64] // id is chunkId
	val -> ChunkInfo

ChunkInfo
	ChunkName string
	map[string]bool // key is nodeIPs // contains all the nodes which have this chunk
	map[string]bool // key is nodeIPs // contains all the nodes which do not have this chunk

*/

type FileInfo struct {
	ExpectedChunks uint64
	ChunkInfo      map[uint64]*ChunkInfo
}

type ChunkInfo struct {
	ChunkName   string
	ChunkNumber uint64
	ChunkSize   uint64
	// this is a list because it becomes easier to get a random node which will then send replicas to
	// different places
	AvailableIn []string
	NotIn       map[string]bool
}

func getEmptyTable(fs *FileSystem) (*FileSystem, map[string]*NodeR, bool) {
	fs.ResetTables()
	fs.dbLock.Lock()
	defer fs.dbLock.Unlock()
	_, err := fs.db.Exec(
		"INSERT INTO Files (name, path, folder, size, parent, numberOfChunks) VALUES(?,?,?,?,?,?);", "/", "/", 1, 0, "NO PARENT", 0,
	)
	if err != nil {
		fs.logger.Error("Error instantiating file system")
		fs.logger.Error(err.Error())
		return nil, nil, false
	}

	fs.logger.Info("File system successfully instantiated")
	return fs, nil, true
}

// GetFileSystem /** bool is 'ok' */
func GetFileSystem(db *sql.DB, logger *logrus.Logger, lock *sync.Mutex, freshTable bool) (*FileSystem, map[string]*NodeR, bool) {
	db.Exec("PRAGMA foreign_keys=ON")
	fs := &FileSystem{db: db, logger: logger, dbLock: lock}

	if freshTable {
		return getEmptyTable(fs)
	}

	fs.dbLock.Lock()
	defer fs.dbLock.Unlock()
	rows, err := db.Query("SELECT EXISTS (SELECT 1 FROM Files);")
	defer rows.Close()

	if err != nil {
		logger.Error("Error in checking DB has values present")
		panic("Error")
	}

	var exists int
	rows.Next()

	if rows.Scan(&exists); exists == 0 {
		return getEmptyTable(fs)
	}

	rows.Close()

	qRows, qErr := db.Query(
		"SELECT * FROM StorageNodes",
	)
	defer qRows.Close()

	if qErr != nil {
		logger.Error("Error in executing shownodes query")
		logger.Error(qErr.Error())
		return nil, nil, false
	}

	var (
		id           string
		ip           string
		isActive     bool
		totalSpace   uint64
		usedSpace    uint64
		totalRequest int64
	)

	nodeMap := make(map[string]*NodeR)

	for qRows.Next() {
		err := qRows.Scan(&id, &ip, &isActive, &totalSpace, &usedSpace, &totalRequest)
		if err != nil {
			logger.Error("Error in trying to scan qRows in shownodes")
			logger.Error(err.Error())
			return nil, nil, false
		}

		node := &NodeR{
			Id:                id,
			Active:            true,
			ConnectionAddress: ip,
			db:                db,
			LastHeartBeat:     time.Now(),
			TotalSize:         totalSpace,
			UsedSpace:         usedSpace,
			logger:            logger,
			dbLock:            lock,
		}

		nodeMap[id] = node
	}

	logger.Info("Shownodes command is successful")
	return fs, nodeMap, true

}

// MKDIR /** bool is 'ok' */
func (f *FileSystem) MKDIR(parentDir string, dirName string) bool {
	if !f.CheckDirExist(parentDir) {
		f.logger.Error("Current dir does not exist, MKDIR unsuccessful")
		return false
	}

	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	dirPath := filepath.Join(parentDir, dirName)
	_, err := f.db.Exec(
		"INSERT INTO Files (name, path, folder, size, parent, numberOfChunks, status) VALUES(?,?,?,?,?,?,?);", dirName, dirPath, 1, 0, parentDir, 0, "READY",
	)

	if err != nil {
		f.logger.Error("Error in making dir")
		f.logger.Error(err.Error())
		return false
	}

	f.logger.Info("MKDIR Successful")
	return true
}

// LS /** bool is 'ok' */
func (f *FileSystem) LS(dirname string) (string, bool) {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT name, path, folder, size, status FROM Files where parent = ?", dirname,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in executing LS query")
		f.logger.Error(qErr.Error())
		return "", false
	}

	var (
		name     string
		path     string
		isFolder int
		size     int64
		status   string
	)

	var b bytes.Buffer
	buffer := bufio.NewWriter(&b)

	t := table.NewWriter()
	t.SetStyle(table.StyleColoredDark)
	t.SetOutputMirror(buffer)
	t.AppendHeader(table.Row{"Name", "Path", "Parent", "Status", "Folder", "Size"})

	for rows.Next() {
		err := rows.Scan(&name, &path, &isFolder, &size, &status)
		if err != nil {
			f.logger.Error("Error in trying to scan rows")
			f.logger.Error(err.Error())
			return "", false
		}

		t.AppendRows([]table.Row{
			{name, path, dirname, status, isFolder, size},
		})

		t.AppendSeparator()
	}

	t.Render()
	buffer.Flush()
	f.logger.Info("LS Command is Successful")
	return b.String(), true
}

// CD /** bool is 'ok' */
func (f *FileSystem) CD(currentDir string, query string) (string, bool) {
	currentDirExists := f.CheckDirExist(currentDir)

	if !currentDirExists {
		f.logger.Error("Current dir does not exist")
		return "", false
	}

	targetDir := filepath.Join(currentDir, query)
	targetDirExists := f.CheckDirExist(targetDir)

	if !targetDirExists {
		f.logger.Error("Target dir does not exist")
		return "", false
	}

	return targetDir, true
}

// PWD /** bool is 'ok' */
func (f *FileSystem) PWD(currentDir string, query string) (string, bool) {
	if query == "" {
		return currentDir, true
	}

	targetDir := filepath.Join(currentDir, query)
	targetDirExists := f.CheckDirExist(targetDir)

	if !targetDirExists {
		f.logger.Error("Target dir does not exist")
		return "", false
	}

	return targetDir, true
}

func (f *FileSystem) CheckDirExist(dir string) bool {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT EXISTS(SELECT 1 FROM Files WHERE path= ? LIMIT 1);", dir,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in checking for if dir exist")
		f.logger.Error(qErr.Error())
		return false
	}

	var targetExists string
	rows.Next()
	err := rows.Scan(&targetExists)
	if err != nil {
		f.logger.Error("Error in trying to scan row")
		f.logger.Error(err.Error())
		return false
	}

	if targetExists == "0" {
		return false
	}

	return true
}

// InsertFile /** bool is 'ok' */
func (f *FileSystem) InsertFile(fileName string, filePath string, chunkCount uint64, parentDir string, fileSize uint64, fileStatus string) (uint64, bool) {
	if !f.CheckDirExist(parentDir) {
		f.logger.Error("Current dir does not exist, InsertFile unsuccessful")
		return 0, false
	}

	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	insertRes, insertErr := f.db.Exec(
		"INSERT INTO Files (Name, Path, Folder, Size, Parent, NumberOfChunks, Status) VALUES(?,?,?,?,?,?,?);", fileName, filePath, 0, fileSize, parentDir, chunkCount, fileStatus,
	)

	if insertErr != nil {
		f.logger.Error("Error in inserting file")
		f.logger.Error(insertErr.Error())
		return 0, false
	}

	f.logger.Info("Insert file Successful")

	fileId, _ := insertRes.LastInsertId()

	return uint64(fileId), true
}

func (f *FileSystem) InsertChunk(fileId uint64, chunkNumber uint64, chunkName string, nodeIp string, chunkSize uint64) bool {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	_, err := f.db.Exec(
		"INSERT INTO Chunks (File, StorageNode, Size, ChunkNumber, ChunkName) VALUES(?,?,?,?,?);", fileId, nodeIp, chunkSize, chunkNumber, chunkName,
	)

	if err != nil {
		f.logger.Error("Error in entering chunkInfo to db")
		f.logger.Error(err.Error())
		return false
	}

	return true
}

func (f *FileSystem) ExpectedChunkCount(fileId uint64) (uint64, bool) {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT NumberOfChunks FROM Files WHERE ID= ?;", fileId,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in checking for if dir exist")
		f.logger.Error(qErr.Error())
		return 0, false
	}

	var chunkCount uint64
	rows.Next()
	err := rows.Scan(&chunkCount)
	if err != nil {
		f.logger.Error("Error in trying to scan row")
		f.logger.Error(err.Error())
		return 0, false
	}
	return chunkCount, true
}

// GetFileChunkAddresses // map[ChunkNumber]name,ipList
func (f *FileSystem) GetFileChunkAddresses(fileId uint64) (map[uint64]*dbChunkCount, bool) {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT ChunkName, ChunkNumber, StorageNode FROM Chunks WHERE File= ?;", fileId,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in checking for if dir exist")
		f.logger.Error(qErr.Error())
		return nil, false
	}

	countMap := make(map[uint64]*dbChunkCount)

	var (
		chunkName   string
		chunkNumber uint64
		nodeIp      string
	)

	for rows.Next() {
		scanErr := rows.Scan(&chunkName, &chunkNumber, &nodeIp)

		if scanErr != nil {
			f.logger.Error("Error scanning for file chunk count")
			f.logger.Error(scanErr.Error())
			return nil, false
		}

		if _, exist := countMap[chunkNumber]; !exist {
			countMap[chunkNumber] = &dbChunkCount{}
			countMap[chunkNumber].NodeIps = make([]string, 0)
		}

		countMap[chunkNumber].ChunkName = chunkName
		countMap[chunkNumber].NodeIps = append(countMap[chunkNumber].NodeIps, nodeIp)
	}
	return countMap, true
}

func (f *FileSystem) GetFileStatus(fileId uint64) (FileStatus, bool) {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	f.logger.Debugf("Getting status of file with id %d", fileId)
	rows, qErr := f.db.Query(
		"SELECT Status FROM Files WHERE ID= ?;", fileId,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in checking for if dir exist")
		f.logger.Error(qErr.Error())
		return 0, false
	}

	var status string
	rows.Next()
	err := rows.Scan(&status)
	if err != nil {
		f.logger.Error("Error in trying to scan row")
		f.logger.Error(err.Error())
		return 0, false
	}

	if status == "IN PROGRESS" {
		return InProgress, true
	} else if status == "LOST" {
		return Lost, true
	} else if status == "READY" {
		return Ready, true
	}

	return 0, false
}

func (f *FileSystem) ResetTables() {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, err := f.db.Query("SELECT name FROM sqlite_master where type='table'")
	defer rows.Close()
	if err != nil {
		f.logger.Error("Error in getting all tables")
		f.logger.Error(err.Error())
		return
	}

	tableNames := make([]string, 0)
	for rows.Next() {
		var name string
		rows.Scan(&name)

		if name != "sqlite_sequence" {
			f.logger.Info("Got to delete from table " + name)
			tableNames = append(tableNames, name)
		}
	}

	for _, name := range tableNames {
		_, delerr := f.db.Exec("DELETE FROM " + name)
		if delerr != nil {
			f.logger.Error("Error deleting " + name + " from database")
			f.logger.Error(delerr.Error())
			return
		}
	}

	f.logger.Info("Successfully emptied rows the tables")
}

func (f *FileSystem) ResetAndCloseConn() {
	f.ResetTables()
	f.CloseConn()
}

func (f *FileSystem) CloseConn() {
	f.db.Close()
	f.logger.Info("Successfully closed connection to db")
}

func (f *FileSystem) UpdateFileStatus(fileId uint64, status string) bool {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	_, qErr := f.db.Exec(
		"UPDATE Files SET Status = ? WHERE ID= ?;", status, fileId,
	)

	if qErr != nil {
		f.logger.Error("Error in checking for if dir exist")
		f.logger.Error(qErr.Error())
		return false
	}

	return true
}

func (f *FileSystem) CheckIfTableEmpty() bool {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, _ := f.db.Query(
		"SELECT count(*) FROM Chunks;",
	)
	defer rows.Close()

	var count uint64
	rows.Next()
	rows.Scan(&count)

	if count == 0 {
		return false
	}

	return true
}

func (f *FileSystem) GetAllChunksInfo() (map[uint64]*FileInfo, bool) {

	// map[uint64]*FileInfo // id is fileId
	/* FileInfo
	ExpectedChunks uint64
	map[uint64] // id is chunkId
		val -> ChunkInfo

	ChunkInfo
		ChunkName string
		map[string]bool // key is nodeIPs // contains all the nodes which have this chunk
		map[string]bool // key is nodeIPs // contains all the nodes which do not have this chunk

	*/

	allIPsMap, _ := f.GetAvailableIPs(true)

	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT ID, File, StorageNode, ChunkNumber, ChunkName, Size FROM Chunks;",
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in checking for if dir exist")
		f.logger.Error(qErr.Error())
		return nil, false
	}

	infoMap := make(map[uint64]*FileInfo)

	var (
		chunkId     uint64
		fileId      uint64
		nodeIp      string
		chunkNumber uint64
		chunkName   string
		chunkSize   uint64
	)

	for rows.Next() {
		scanErr := rows.Scan(&chunkId, &fileId, &nodeIp, &chunkNumber, &chunkName, &chunkSize)

		if scanErr != nil {
			f.logger.Error("Error scanning for file chunk count")
			f.logger.Error(scanErr.Error())
			return nil, false
		}

		if _, exist := infoMap[fileId]; !exist {
			fileInfo := &FileInfo{
				ChunkInfo: make(map[uint64]*ChunkInfo),
			}
			infoMap[fileId] = fileInfo
		}

		fileInfo := infoMap[fileId] // is a pointer to file info // makes it easier to read code

		if _, exist := fileInfo.ChunkInfo[chunkId]; !exist {
			chunkInfo := &ChunkInfo{
				ChunkName:   chunkName,
				ChunkNumber: chunkNumber,
				ChunkSize:   chunkSize,
				// todo get all storage nodes and initialize them here
				// available in will have defaults of false, which we make true when we get a node
				// not in will have a default of true, which we make false when we get a node
				AvailableIn: make([]string, 0),
				NotIn:       allIPsMap,
			}
			fileInfo.ChunkInfo[chunkId] = chunkInfo
		}

		chunkInfo := fileInfo.ChunkInfo[chunkId]

		chunkInfo.AvailableIn = append(chunkInfo.AvailableIn, nodeIp)
		chunkInfo.NotIn[nodeIp] = false
	}

	return infoMap, true
}

// GetAvailableIPs Both the map and strings will have the same data, it is up to the user
// 					what they want to use/**
func (f *FileSystem) GetAvailableIPs(mapDefaultValue bool) (map[string]bool, bool) {
	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT IP FROM StorageNodes",
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in executing shownodes query")
		f.logger.Error(qErr.Error())
		return nil, false
	}

	ipMap := make(map[string]bool)
	ipList := make([]string, 0)

	var (
		ip string
	)

	for rows.Next() {
		err := rows.Scan(&ip)
		if err != nil {
			f.logger.Error("Error in trying to scan rows in shownodes")
			f.logger.Error(err.Error())
			return nil, false
		}

		ipMap[ip] = mapDefaultValue
		ipList = append(ipList, ip)
	}

	return ipMap, true
}

func (f *FileSystem) GetCountOfChunk(name string) (*ChunkInfo, bool) {
	allIPsMap, _ := f.GetAvailableIPs(true)

	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT * FROM Chunks WHERE ChunkName = ?", name,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error in getting chunk count")
	}

	var (
		chunkId     uint64
		fileId      uint64
		nodeIp      string
		chunkNumber uint64
		chunkName   string
		chunkSize   uint64
	)

	chunkInfo := &ChunkInfo{
		ChunkName:   name,
		ChunkNumber: 0,
		ChunkSize:   0,
		AvailableIn: make([]string, 0),
		NotIn:       allIPsMap,
	}

	for rows.Next() {
		scanErr := rows.Scan(&chunkId, &fileId, &nodeIp, &chunkSize, &chunkNumber, &chunkName)

		if scanErr != nil {
			f.logger.Error("Error scanning for file chunk count")
			f.logger.Error(scanErr.Error())
			return nil, false
		}

		chunkInfo.ChunkNumber = chunkNumber
		chunkInfo.ChunkSize = chunkSize
		chunkInfo.AvailableIn = append(chunkInfo.AvailableIn, nodeIp)
		chunkInfo.NotIn[nodeIp] = false
	}

	//f.logger.Debugf("Chunk %s is available in following nodes", name)
	//f.logger.Debug(chunkInfo)

	return chunkInfo, true
}

func (f *FileSystem) CheckFileReadyToDownload(path string) (bool, string, string, uint64, uint64) {
	if !f.CheckDirExist(path) { // every file has a filePath // method naming is bad
		return false, "Target dir does not exist", "", 0, 0
	}

	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	rows, qErr := f.db.Query(
		"SELECT Name, ID, Folder, NumberOfChunks, Status, Size FROM Files WHERE Path = ?", path,
	)
	defer rows.Close()

	if qErr != nil {
		f.logger.Error("Error executing checkFileExists query")
		f.logger.Error(qErr.Error())
		return false, "Error executing query", "", 0, 0
	}

	var (
		fileName       string
		fileId         uint64
		isFolder       bool
		numberOfChunks uint64
		status         string
		size           uint64
	)
	rows.Next()
	rows.Scan(&fileName, &fileId, &isFolder, &numberOfChunks, &status, &size)

	if isFolder {
		f.logger.Error("User wants to download a folder, operation not supported")
		return false, "Cannot download a folder", "", 0, 0
	}

	if status != "READY" {
		f.logger.Error("Cannot download a file which is not ready")
		return false, "File if is not ready", "", 0, 0
	}

	return true, "", fileName, fileId, size
}

func (f *FileSystem) DeleteFile(path string) (bool, string) {
	ok, errMessage, _, _, _ := f.CheckFileReadyToDownload(path)

	if !ok {
		return false, errMessage
	}

	f.dbLock.Lock()
	defer f.dbLock.Unlock()
	_, qErr := f.db.Exec(
		"DELETE FROM Chunks WHERE File = (Select ID FROM Files WHERE Path = ?);DELETE FROM Files WHERE Path = ?", path, path,
	)

	if qErr != nil {
		f.logger.Debugf("Error in deleting from database")
		f.logger.Debug(qErr.Error())
		return false, qErr.Error()
	}

	return true, ""
}
