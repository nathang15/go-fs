package pkg

import "time"

type nodeMetadata struct {
	NodeAddress   string
	Files         []string
	TotalFileSize int32
}

type WriteTransaction struct {
	FileName        string
	FileVersion     int32
	RequestID       string
	PreferenceNodes []string
	RegisteredTime  time.Time
}

type TempFile struct {
	FileName     string
	FileVersion  int32
	RequestID    string
	TempFileName string
	FileSize     int32
}

type FileMetadata struct {
	FileName        string    //file name
	FileVersion     int32     //file version
	FileSize        int32     //file size
	RegisteredTime  time.Time //latest registered time
	PreferenceNodes []string  //list of its replica nodes
}

type Server struct{}

var files []FileMetadata
var nodes []nodeMetadata
var pendingWriteTransactions []WriteTransaction
var temporaryFiles []TempFile
var masterNode string
var client Client
var electionTerm int32
