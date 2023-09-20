package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/kyokomi/emoji/v2"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Node struct {
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

// InitNode /** bool is 'ok' */
func InitNode(connectionAddress string, totalSize uint64, usedSpace uint64, logger *log.Logger, db *sql.DB, dbLock *sync.Mutex) (*Node, bool) {
	dbLock.Lock()
	defer dbLock.Unlock()
	insRes, err := db.Exec(
		"INSERT INTO StorageNodes (Ip, IsActive, TotalSpace, UsedSpace, TotalRequest) VALUES(?,?,?,?,?);", connectionAddress, 1, totalSize, usedSpace, 0,
	)

	if err != nil {
		logger.Error("Error in initializing storage nodes")
		logger.Error(err.Error())
		return nil, false
	}

	id, _ := insRes.LastInsertId()

	node := &Node{
		Id:                fmt.Sprint(id),
		Active:            true,
		ConnectionAddress: connectionAddress,
		db:                db,
		LastHeartBeat:     time.Now(),
		TotalSize:         totalSize,
		UsedSpace:         usedSpace,
		logger:            logger,
		dbLock:            dbLock,
	}
	logger.Info("Storage nodes initialized successfully")
	return node, true
}

func (n *Node) MarkOffline() bool {
	n.dbLock.Lock()
	defer n.dbLock.Unlock()
	n.logger.Infof("Marking node %s as inactive", n.Id)
	_, err := n.db.Exec(
		"DELETE FROM StorageNodes WHERE ID=?;", n.Id,
	)

	n.logger.Infof("Deleting chunk info of node %s", n.ConnectionAddress)
	_, cErr := n.db.Exec(
		"DELETE FROM Chunks WHERE StorageNode=?;", n.ConnectionAddress)

	if err != nil {
		n.logger.Errorf("Unable to make node %s as inactive (error message below)", n.Id)
		n.logger.Error(err.Error())
		return false
	}

	if cErr != nil {
		n.logger.Errorf("Unable to make node %s as inactive (error message below)", n.Id)
		n.logger.Error(err.Error())
		return false
	}

	/**
	update files set status =1
	where fid in (
		select fid from files f where fid = ?
		and (select count(ID) from chunks where File = f.fid) = f.NumberOfChunks
	)
	*/
	n.logger.Infof("Successfully deleted node %s with ip %s", n.Id, n.ConnectionAddress)
	return true
}

func (n *Node) UpdateUsedSpace() bool {
	n.dbLock.Lock()
	defer n.dbLock.Unlock()
	_, err := n.db.Exec(
		"UPDATE StorageNodes SET UsedSpace = ? WHERE ID = ?", n.UsedSpace, n.Id,
	)

	if err != nil {
		n.logger.Errorf("Unable to update used space of node %s with ip %s (error message below)", n.Id, n.ConnectionAddress)
		n.logger.Error(err.Error())
		return false
	}

	n.logger.Infof("Successfully updated used space of node %s with ip %s", n.Id, n.ConnectionAddress)
	return true
}

func (n *Node) UpdateTotalRequest() bool {
	n.dbLock.Lock()
	defer n.dbLock.Unlock()
	_, err := n.db.Exec(
		"UPDATE StorageNodes SET TotalRequest = ? WHERE ID = ?", n.TotalRequest, n.Id,
	)

	if err != nil {
		n.logger.Errorf("Unable to update total request of node %s with ip %s (error message below)", n.Id, n.ConnectionAddress)
		n.logger.Error(err.Error())
		return false
	}

	n.logger.Infof("Successfully updated total request of node %s with ip %s", n.Id, n.ConnectionAddress)
	return true
}

func (n *Node) GetAvailableSpace() uint64 {
	return n.TotalSize - n.UsedSpace
}

// PrintNodes /** bool is 'ok' */
func PrintNodes(db *sql.DB, logger *log.Logger, dbLock *sync.Mutex, nodeMap map[string]*Node) (string, bool) {
	dbLock.Lock()
	defer dbLock.Unlock()
	rows, qErr := db.Query(
		"SELECT * FROM StorageNodes",
	)
	defer rows.Close()

	if qErr != nil {
		logger.Error("Error in executing shownodes query")
		logger.Error(qErr.Error())
		return "", false
	}

	var (
		id           string
		ip           string
		isActive     bool
		totalSpace   uint64
		usedSpace    uint64
		totalRequest int64
	)

	var b bytes.Buffer
	buffer := bufio.NewWriter(&b)

	t := table.NewWriter()
	t.SetStyle(table.StyleColoredYellowWhiteOnBlack)
	t.SetOutputMirror(buffer)
	t.AppendHeader(table.Row{"ID", "IP", "Active", "TotalSpace", "UsedSpace", "TotalRequest"})
	t.Style()

	for rows.Next() {
		err := rows.Scan(&id, &ip, &isActive, &totalSpace, &usedSpace, &totalRequest)
		if err != nil {
			logger.Error("Error in trying to scan rows in shownodes")
			logger.Error(err.Error())
			return "", false
		}

		cool := emoji.Sprint(":face_with_monocle:")
		notcool := emoji.Sprint(":poop:")

		if isActive {
			t.AppendRows([]table.Row{
				{id, ip, cool, totalSpace, nodeMap[id].UsedSpace, nodeMap[id].TotalRequest},
			})
		} else {
			t.AppendRows([]table.Row{
				{id, ip, notcool, totalSpace, nodeMap[id].UsedSpace, nodeMap[id].TotalRequest},
			})
		}
		//t.AppendSeparator()
	}

	t.Render()
	buffer.Flush()

	logger.Info("Shownodes command is successful")
	return b.String(), true
}

func GetAvailableIps(nodeMap map[string]*Node, logger *log.Logger) []string {
	ipList := make([]string, 0)

	logger.Debugf("len of available ips is %d", len(nodeMap))
	for _, node := range nodeMap {
		if node.Active {
			ipList = append(ipList, node.ConnectionAddress)
		}
	}

	return ipList
}
