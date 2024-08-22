package pkg

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nathang15/go-fs/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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

type relocatedFileMetadata struct {
	fileName        string
	fileSize        int32
	fileVersion     int32
	srcReplicaNode  string
	destReplicaNode string
}

type Server struct{}
type grpcFileServer struct {
	pb.UnimplementedFileServiceServer
}
type readAcquireResult struct {
	status     bool
	version    int32
	targetNode string
}

var files []FileMetadata
var nodes []nodeMetadata
var pendingWriteTransactions []WriteTransaction
var temporaryFiles []TempFile
var masterNode string
var client Client

// track the current term of the election. Increase by 1 when a new election is started
var electionTerm int32

// True when election is in process. False otherwise
var onElectionProcess bool

// track the order of backup requests sent from master node to its members
var backupCounter int32

// indicate whether the election is done. True means in progress. False otherwise
var voteRecords []int32

// Stop the current service. Reset all global variable
func (server *Server) Stop() {
	electionTerm = 0
	backupCounter = 0
	masterNode = ""
}

// Start the file server
func (server *Server) Start(master string, eTerm int32, initFiles []FileMetadata, initPendingWriteTransactions []WriteTransaction) {
	masterNode = master
	electionTerm = eTerm
	server.UpdateMetada(initFiles, initPendingWriteTransactions)
}

// Update all the information of file operations for master node
func (server *Server) UpdateMetada(initFiles []FileMetadata, initPendingWriteTransactions []WriteTransaction) {
	files = initFiles
	pendingWriteTransactions = initPendingWriteTransactions

	updateNodes()
}

func (server *Server) StartElection() {
	onElectionProcess = true

	electionAttempt := 0
	for true {
		// stop self-promoting process when a node is voted as master
		if !onElectionProcess {
			break
		}

		isDone := server.SelfPromote()
		if isDone {
			break
		}

		// retry in case election fails
		// each leader candidate will have different wait time to avoid looping self election

		electionAttempt++
		// parse ip address to number
		ipNumbers := strings.Split(config.IPAddress, ".")
		var seed int64
		for _, ipNumber := range ipNumbers {
			ipNumberVal, _ := strconv.Atoi(ipNumber)
			seed = seed + int64(ipNumberVal)
		}
		seed = seed + time.Now().UnixNano()
		s1 := rand.NewSource(seed)
		r1 := rand.New(s1)

		n := r1.Intn(10)

		waitingTime := 200 + n*100
		log.Printf("Election has failed, try again in '%d' ms, attempt= '%d' ... \n", waitingTime, electionAttempt)

		time.Sleep(time.Duration(waitingTime) * time.Millisecond)
	}
}

// Start sending requests to other nodes to self-promote as new master
func (server *Server) SelfPromote() bool {
	// self motivate to be leader of the next term
	electionTerm = electionTerm + 1

	c := make(chan bool)

	go func() {
		isElectionSuccessful := sendVotingRequests()
		c <- isElectionSuccessful
	}()

	// Listen on the current channel AND the timeout channel. Take in the result of whichever happens first
	select {
	case res := <-c:
		// if the election is successful, set it as master and send out the result to all other nodes
		if res && onElectionProcess {
			//electionIsOn = false
			masterNode = config.IPAddress
			onElectionProcess = false
			sendAnnouncementRequests()
			return true
		}
	// TIMEOUT
	case <-time.After(300 * time.Millisecond):
		fmt.Println("Unable to select leader before timeout. The process will be restarted in few milliseconds")
		return false
	}
	return false
}

// Transfer all replicated files of this node to other nodes when it leaves
func (server *Server) Offboarding(nodeleaveAddress string) {
	startOffboardTime := time.Now()
	log.Printf("Start offboarding process for node '%s'.", nodeleaveAddress)
	if nodeleaveAddress == masterNode {
		// promote another node to be master
		followers := membershipService.GetFollowerAddresses()
		for _, member := range followers {
			go sendPromoteRequest(followers[0], member)
		}
	}

	//Rearrange replicated files on the leaving node to the other nodes
	var relocatedFileNames []string
	for _, node := range nodes {
		if node.NodeAddress != nodeleaveAddress {
			continue
		}
		relocatedFileNames = node.Files
		break
	}

	var relocatedFiles []relocatedFileMetadata
	for _, relocatedFileName := range relocatedFileNames {
		rMeta := relocatedFileMetadata{
			fileName:        relocatedFileName,
			fileSize:        0,
			srcReplicaNode:  "",
			destReplicaNode: "",
		}
		for _, file := range files {
			if relocatedFileName == file.FileName {
				rMeta.fileSize = file.FileSize
				rMeta.fileVersion = file.FileVersion
				break
			}
		}
		relocatedFiles = append(relocatedFiles, rMeta)
	}

	// remove leaving node from file's preference nodes
	for idx, file := range files {
		pendingIndex := -1

		for i := 0; i < len(file.PreferenceNodes); i++ {
			if file.PreferenceNodes[i] == nodeleaveAddress {
				pendingIndex = i
				break
			}
		}
		if pendingIndex == -1 {
			continue
		}

		files[idx].PreferenceNodes = append(files[idx].PreferenceNodes[:pendingIndex], files[idx].PreferenceNodes[pendingIndex+1:]...)
	}

	updateNodes()

	// specify destination for the relocated files
	for removedIdx, relocatedfile := range relocatedFiles {
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].TotalFileSize < nodes[j].TotalFileSize
		})

		for idx, file := range files {
			if file.FileName != relocatedfile.fileName {
				continue
			}

			for newIdx := 0; newIdx < len(nodes); newIdx++ {
				if relocatedFiles[removedIdx].srcReplicaNode != "" && relocatedFiles[removedIdx].destReplicaNode != "" {
					break
				}
				if contains(file.PreferenceNodes, nodes[newIdx].NodeAddress) {
					if relocatedFiles[removedIdx].srcReplicaNode == "" {
						relocatedFiles[removedIdx].srcReplicaNode = nodes[newIdx].NodeAddress
					}
				} else {
					if relocatedFiles[removedIdx].destReplicaNode == "" {
						relocatedFiles[removedIdx].destReplicaNode = nodes[newIdx].NodeAddress
						files[idx].PreferenceNodes = append(files[idx].PreferenceNodes, nodes[newIdx].NodeAddress)
					}
				}
			}
			updateNodes()
			break
		}
	}

	c := make(chan bool)

	// sending reallocate request to the target nodes
	for _, relocatedfile := range relocatedFiles {
		go sendBackupFileRequest(relocatedfile, c)
	}

	server.Backup()

	for i := 0; i < len(relocatedFiles); i++ {
		<-c
	}

	log.Println(" ")
	log.Printf("********************** Start Re-Replication Report **********************")
	log.Printf("Report for re-replicate process for node '%s'", nodeleaveAddress)
	log.Printf("Start time: '%s'", ConvertTimeToLongString(startOffboardTime))
	log.Printf("End time: '%s'", ConvertTimeToLongString(time.Now()))
	log.Printf("Total runtime: '%d' ms", time.Now().Sub(startOffboardTime).Milliseconds())

	log.Println("---------------------------------------------------------")
	var totalSize int32
	for _, removedFile := range relocatedFiles {
		log.Printf("Moved file '%s', version '%d', size '%d' from node '%s' to node '%s'", removedFile.fileName, removedFile.fileVersion, removedFile.fileSize, removedFile.srcReplicaNode, removedFile.destReplicaNode)
		totalSize = totalSize + removedFile.fileSize
	}
	log.Println("Total file size is ", totalSize)

	log.Printf("******************* End Re-replication Report **********************")
	log.Println(" ")
}

// Update all the information of file operations for master node
func (server *Server) UpdateReplicaNodes(newMember string) {
	// nodemt = node metadata
	nodemt := nodeMetadata{
		NodeAddress:   newMember,
		TotalFileSize: 0,
		Files:         []string{},
	}
	nodes = append(nodes, nodemt)
}

// Backup all infomration about file operations
func (server *Server) Backup() {

	log.Printf("Sending backup request to all members.\n")

	var bpFiles []*pb.BackupRequest_FileMetadata
	var bpPendingWriteTransactions []*pb.BackupRequest_WriteTransaction

	for _, file := range files {
		file := &pb.BackupRequest_FileMetadata{
			FileName:        file.FileName,
			FileSize:        file.FileSize,
			FileVersion:     file.FileVersion,
			PreferenceNodes: file.PreferenceNodes,
		}
		bpFiles = append(bpFiles, file)
	}

	for _, pendingWriteTransaction := range pendingWriteTransactions {
		bpPendingWriteTransaction := &pb.BackupRequest_WriteTransaction{
			FileName:        pendingWriteTransaction.FileName,
			FileVersion:     pendingWriteTransaction.FileVersion,
			RequestID:       pendingWriteTransaction.RequestID,
			PreferenceNodes: pendingWriteTransaction.PreferenceNodes,
			RegisteredTime:  pendingWriteTransaction.RegisteredTime.UnixNano() / int64(time.Millisecond),
		}
		bpPendingWriteTransactions = append(bpPendingWriteTransactions, bpPendingWriteTransaction)
	}

	backupCounter = backupCounter + 1
	//gRPC call
	backupRequest := &pb.BackupRequest{
		Files:                    bpFiles,
		PendingWriteTransactions: bpPendingWriteTransactions,
		Counter:                  backupCounter,
	}

	for _, member := range membershipService.GetFollowerAddresses() {
		requestSize := proto.Size(backupRequest)
		log.Println("************** Sending backup request to ", member, ": size = ", requestSize)
		cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", member, config.TCPPort), grpc.WithInsecure())
		if err != nil {
			errorMessage := fmt.Sprintf("could not connect to target node at %s. Error: %v\n", member, err.Error())
			log.Printf("Cannot send backup to node '%s'. Error: %s\n", member, errorMessage)
			break
		}

		defer cc.Close()

		// timeout mechanism for gRPC call
		duration := time.Duration(config.Timeout) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		c := pb.NewFileServiceClient(cc)

		backupResponse, err1 := c.Backup(ctx, backupRequest)
		responseSize := proto.Size(backupResponse)
		logging(fmt.Sprintln("************** Send backup response to ", member, ": size = ", responseSize))
		if err1 != nil {
			log.Println("Unable to send backup to node ", member, ".Error: ", err1)
		}
	}

	log.Printf("Successfully sent backup to all nodes.\n")
}

//-------------------------------------------------GRPC APIs-------------------------------------------------------------------

// Locate file
func (*grpcFileServer) Locate(ctx context.Context, req *pb.LocateRequest) (*pb.LocateResponse, error) {
	log.Printf("Receive Locate request for file '%s'.\n", req.GetFileName())
	filename := req.GetFileName()
	file := lookupFileMetadata(filename)

	if file.FileName == "" {
		err := fmt.Sprintf("Unable to locate file %s\n", filename)
		log.Println(err)
		return nil, status.Errorf(codes.Internal, err)
	}

	var replicas []*pb.LocateResponse_PreferenceNode

	for _, replica := range file.PreferenceNodes {
		rep := &pb.LocateResponse_PreferenceNode{
			NodebyAddress: replica,
		}
		replicas = append(replicas, rep)
	}

	resFile := &pb.LocateResponse{
		FileName:    file.FileName,
		FileSize:    file.FileSize,
		FileVersion: file.FileVersion,
		Replicas:    replicas,
	}

	log.Printf("Sent Locate response for file '%s', replicated on '%s'.\n", filename, replicas)

	return resFile, nil
}

// List All files
func (*grpcFileServer) ListAll(ctx context.Context, req *pb.Empty) (*pb.AllFilesResponse, error) {
	log.Println("Receive List All request.")

	var fsfiles []*pb.LocateResponse

	for _, file := range files {
		var replicas []*pb.LocateResponse_PreferenceNode
		for _, replica := range file.PreferenceNodes {
			rep := &pb.LocateResponse_PreferenceNode{
				NodebyAddress: replica,
			}
			replicas = append(replicas, rep)
		}
		file := &pb.LocateResponse{
			FileName:    file.FileName,
			FileSize:    file.FileSize,
			FileVersion: file.FileVersion,
			Replicas:    replicas,
		}
		fsfiles = append(fsfiles, file)
	}

	allFilesResponse := &pb.AllFilesResponse{
		Files: fsfiles,
	}

	log.Println("Sent LIST ALL response")

	return allFilesResponse, nil
}

// DeleteAll
func (*grpcFileServer) DeleteAll(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	fileHandler.ClearDb()
	server.UpdateMetada([]FileMetadata{}, []WriteTransaction{})

	return &pb.Empty{}, nil
}

// Vote a node to be leader
func (*grpcFileServer) Vote(ctx context.Context, req *pb.VotingRequest) (*pb.VotingResponse, error) {
	eTerm := req.GetElectionTerm()
	candidateAddress := req.GetCandidateAddress()

	log.Printf("Receive Vote request from node '%s' to self-promote as the new master of election term '%d'.\n", candidateAddress, eTerm)

	alreadyVoted := false
	for _, voteRecord := range voteRecords {
		if voteRecord == eTerm {
			alreadyVoted = true
		}
	}

	if alreadyVoted {
		log.Printf("Unable to send Vote response to node '%s'.\n", candidateAddress)

		return &pb.VotingResponse{Accepted: false}, nil
	}
	voteRecords = append(voteRecords, eTerm)
	log.Printf("Sent Vote response to node '%s'.\n", candidateAddress)

	return &pb.VotingResponse{Accepted: true}, nil
}

// Announce a node the be the new leader (master)
func (*grpcFileServer) Announce(ctx context.Context, req *pb.AnnouncementRequest) (*pb.Empty, error) {
	newMasterAddress := req.GetNewMasterAddress()

	log.Printf("Receive Announce request from node '%s' to confirm it to be the new master.\n", newMasterAddress)

	//reset the vote records
	voteRecords = []int32{}
	masterNode = newMasterAddress
	onElectionProcess = false

	return &pb.Empty{}, nil
}

// Receive a file replica from another node
func (*grpcFileServer) CopyReplica(stream pb.FileService_CopyReplicaServer) error {
	// Save the file on a temporary file buffer
	log.Printf("Receive Copy Replica request.\n")

	// receive file streaming and save it to a temporary file
	guid := uuid.New()
	tempfilename := fmt.Sprintf("___tempfile_%s", guid)
	tempfile := fileHandler.CreateOrOpenFile(fileHandler.FilePath(tempfilename))

	var filename string
	var fileversion int32
	var filesize int32

	totalFileSize := 0
	counting := 0
	log.Printf("copying replica...")
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(codes.Internal, fmt.Sprintf("Failed reading chunks from stream for file %v.", filename))
		}

		nb, err := tempfile.Write(req.GetFilevalue())
		if err != nil {
			log.Printf("Error while reading chunks from stream to file: %v\n", err.Error())
		}
		totalFileSize = totalFileSize + nb
		if counting%10000 == 0 {
			fmt.Print(".")
		}
		counting++
		filename = req.GetFileName()
		fileversion = req.GetFileVersion()
		filesize = req.GetFileSize()
	}

	log.Printf("Write %d byte from stream to file %s \n", totalFileSize, tempfilename)

	fileHandler.Copy(fileHandler.FilePath(tempfilename), fileHandler.FilePath(filename))
	fileHandler.Delete(fileHandler.FilePath(tempfilename))

	//update local replica storage
	newReplica := FileMetadata{
		FileName:    filename,
		FileVersion: fileversion,
		FileSize:    filesize,
	}
	replicaFiles = append(replicaFiles, newReplica)

	log.Printf("Finish Copy Replica request.\n")

	return nil
}

// Handle request to clone a replica to the new target node
func (*grpcFileServer) BackupFile(ctx context.Context, req *pb.BackupFileRequest) (*pb.Empty, error) {
	fileName := req.GetFileName()
	fileVersion := req.GetFileVersion()
	fileSize := req.GetFileSize()
	targetNodeAddress := req.GetTargetNode()
	log.Printf("Receive Backup request to copy replica of file %s to the target node %s.\n", fileName, targetNodeAddress)

	// read file
	file := fileHandler.OpenFile(fileHandler.FilePath(fileName))
	if file == nil {
		log.Printf("Could not open the replica of file %s\n", fileName)
		return &pb.Empty{}, nil
	}

	defer file.Close()

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", targetNodeAddress, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to connect to target node at %s. Error: %v\n", targetNodeAddress, err.Error())
		log.Printf("Unable to proceed with write preparation request for file '%s'. Error: %s\n", fileName, errorMessage)
		return &pb.Empty{}, nil
	}

	defer cc.Close()

	// timeout mechanism
	duration := time.Duration(config.Timeout*100) * time.Second
	_, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	stream, err := c.CopyReplica(context.Background())
	if err != nil {
		log.Printf("Error while calling WritePrepare: %v", err)
		return &pb.Empty{}, nil
	}

	// Read file in a buf of chunk size and then stream it back to client
	buf := make([]byte, config.FileChunkSize)

	totalRequestSize := 0
	writing := true
	for writing {
		_, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		copyReplicaRequest := &pb.CopyReplicaRequest{
			FileName:    fileName,
			Filevalue:   buf,
			FileVersion: fileVersion,
			FileSize:    fileSize,
		}
		totalRequestSize = totalRequestSize + proto.Size(copyReplicaRequest)
		stream.Send(copyReplicaRequest)
	}

	log.Printf("Sent Copy Replica REQUEST from '%s' to '%s', size = '%d'", config.IPAddress, targetNodeAddress, totalRequestSize)

	copyReplicaResponse, _ := stream.CloseAndRecv()
	responseSize := proto.Size(copyReplicaResponse)
	log.Printf("Sent Copy Replica RESPONSE from '%s' to '%s', size = '%d'", config.IPAddress, targetNodeAddress, responseSize)

	log.Printf("Sent file Backup response to copy replica of file %s to the target node %s.\n", fileName, targetNodeAddress)
	return &pb.Empty{}, nil
}

// Accept a promote request
func (*grpcFileServer) Promote(ctx context.Context, req *pb.PromoteRequest) (*pb.Empty, error) {
	newMasterNode := req.GetNewMasterNode()
	log.Printf("Receive PROMOTE request for new master node which is %s.\n", newMasterNode)

	masterNode = newMasterNode
	electionTerm = electionTerm + 1

	return &pb.Empty{}, nil
}

// Backup the local files metadata with the metadata sent by master node
func (*grpcFileServer) Backup(ctx context.Context, req *pb.BackupRequest) (*pb.BackupResponse, error) {
	log.Printf("Receive backup request.\n")

	var initFiles []FileMetadata
	var initPendingWriteTransactions []WriteTransaction

	bpFiles := req.GetFiles()
	bpPendingWriteTransactions := req.GetPendingWriteTransactions()
	counter := req.GetCounter()

	//should not update file metadata if the backup request counter is less than file server's current counter
	if counter <= backupCounter {
		err := fmt.Sprintf("Warning: backup request '%d' will not be proceeded: Out of data than the current backup counter '%d'.\n", counter, backupCounter)
		log.Println(err)
		return &pb.BackupResponse{}, status.Errorf(codes.Internal, err)
	}

	backupCounter = counter

	for _, pbFile := range bpFiles {
		initFile := FileMetadata{
			FileName:        pbFile.FileName,
			FileSize:        pbFile.FileSize,
			FileVersion:     pbFile.FileVersion,
			PreferenceNodes: pbFile.PreferenceNodes,
		}
		initFiles = append(initFiles, initFile)
	}

	for _, bpPendingWriteTransaction := range bpPendingWriteTransactions {
		initPendingWriteTransaction := WriteTransaction{
			FileName:        bpPendingWriteTransaction.FileName,
			FileVersion:     bpPendingWriteTransaction.FileVersion,
			RequestID:       bpPendingWriteTransaction.RequestID,
			PreferenceNodes: bpPendingWriteTransaction.PreferenceNodes,
			RegisteredTime:  time.Unix(0, bpPendingWriteTransaction.RegisteredTime*int64(time.Millisecond)),
		}
		initPendingWriteTransactions = append(initPendingWriteTransactions, initPendingWriteTransaction)
	}

	server.UpdateMetada(initFiles, initPendingWriteTransactions)

	log.Printf("Sent backup response.\n")

	return &pb.BackupResponse{}, nil
}

// Register a write transaction
func (*grpcFileServer) WriteRegister(ctx context.Context, req *pb.WriteRegisterRequest) (*pb.WriteRegisterResponse, error) {
	filename := req.GetFileName()
	log.Printf("Received WRITE REGISTRATION request for file '%s'.\n", filename)
	startRegisteredTime := time.Now()
	var newFileVersion int32
	var preferenceNodes []string

	fileMetadata := lookupFileMetadata(filename)

	var latestTime time.Time

	// Allocate replica nodes for the new file
	if fileMetadata.FileName == "" {
		log.Printf("The file %s is new so master node will allocate new replica nodes for it.\n", filename)
		preferenceNodes = allowcatePreferenceNodes()
		newFileVersion = 1
		// If replica nodes already existed
	} else {
		//use existing replica nodes
		preferenceNodes = fileMetadata.PreferenceNodes
		latestTime = fileMetadata.RegisteredTime
		newFileVersion = fileMetadata.FileVersion + 1
	}

	//push new write transaction into write transactions buffer
	writeTransaction := WriteTransaction{
		FileName:        filename,
		FileVersion:     newFileVersion,
		RequestID:       req.GetRequestId(),
		PreferenceNodes: preferenceNodes,
		RegisteredTime:  time.Now(),
	}

	// needs user interaction if it is not the first write for this file
	// and it is initiated within 1 min from the previous one.
	needUserInteraction := false

	// query all the pending write transaction for the file
	var latestPendingWriteTransaction WriteTransaction
	for _, writeTransaction := range pendingWriteTransactions {
		if writeTransaction.FileName == filename && writeTransaction.FileVersion > latestPendingWriteTransaction.FileVersion {
			latestPendingWriteTransaction = writeTransaction
		}
	}

	// if there is pending transaction for the same file,
	// the new file version must be greater than that pending transaction
	if latestPendingWriteTransaction.FileVersion >= newFileVersion {
		newFileVersion = latestPendingWriteTransaction.FileVersion + 1
		latestTime = latestPendingWriteTransaction.RegisteredTime
	}

	// REPORT
	if newFileVersion > 1 {
		timeDiff := writeTransaction.RegisteredTime.Sub(latestTime).Milliseconds()
		if timeDiff <= 60000 {
			log.Println(" ")
			log.Printf("******************* W-W CONFLICT REPORT **********************")
			log.Printf("Write conflict on file '%s'", filename)
			log.Printf("Prior registered write is for version '%d' at '%s'", newFileVersion-1, ConvertTimeToLongString(latestTime))
			log.Printf("Current registered write is for version '%d' at '%s'", newFileVersion, ConvertTimeToLongString(writeTransaction.RegisteredTime))
			log.Printf("W-W confict process starts at '%s'", ConvertTimeToLongString(startRegisteredTime))
			log.Printf("W-W confict process starts at '%s'", ConvertTimeToLongString(time.Now()))
			log.Printf("W-W confict process takes '%d' microseconds", time.Now().Sub(startRegisteredTime).Microseconds())
			needUserInteraction = true
			log.Printf("******************* END W-W CONFLICT REPORT **********************")
			log.Println(" ")
		}
	}

	pendingWriteTransactions = append(pendingWriteTransactions, writeTransaction)
	server.Backup()

	writeRegistrationResponse := &pb.WriteRegisterResponse{
		PreferenceNodes:     preferenceNodes,
		FileVersion:         newFileVersion,
		NeedUserInteraction: needUserInteraction,
	}

	log.Printf("Sent WRITE REGISTER response for file '%s'. Version: %d, preference nodes: '%s', need user interaction: %v.\n", filename, newFileVersion, preferenceNodes, needUserInteraction)

	return writeRegistrationResponse, nil
}

// Uploading file to replica to prepare for a write transaction
func (*grpcFileServer) WritePrepare(stream pb.FileService_WritePrepareServer) error {
	log.Printf("Receive Write Prepare request.\n")

	// total file size
	var filesize int32
	// receive file streaming and save it to a temporary file
	guid := uuid.New()
	tempfilename := fmt.Sprintf("___tempfile_%s", guid)
	tempfile := fileHandler.CreateOrOpenFile(fileHandler.FilePath(tempfilename))

	var filename string
	var fileversion int32
	var requestID string

	log.Printf("write preparation in progress ...")
	totalSize := 0
	counting := 0
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(codes.Internal, fmt.Sprintf("Failed to read chunks from stream for file: %v due to internal error %s", filename, err))
		}

		nb, err := tempfile.Write(req.GetFileValue())
		if err != nil {
			log.Printf("Error while reading chunks from stream to file: %v\n", err.Error())
			return status.Errorf(codes.Internal, fmt.Sprintf("Error while writing to file %s. Error: %v\n", filename, err))
		}

		totalSize = totalSize + nb

		filesize = filesize + int32(nb)
		filename = req.GetFileName()
		fileversion = req.GetFileVersion()
		requestID = req.GetRequestId()
		if counting%10000 == 0 {
			fmt.Print(".")
		}
		counting++
	}
	log.Printf("Write %d byte from client to file %s \n", totalSize, tempfilename)
	tempfile.Close()

	tempFile := TempFile{
		FileName:     filename,
		FileVersion:  fileversion,
		RequestID:    requestID,
		TempFileName: tempfilename,
		FileSize:     filesize,
	}

	temporaryFiles = append(temporaryFiles, tempFile)

	log.Printf("Sent Write Preparation response for file %s.\n", filename)

	return stream.SendAndClose(&pb.WritePreparationResponse{FileSize: filesize})
}

// End write transaction by committing or aborting
func (*grpcFileServer) WriteEnd(ctx context.Context, req *pb.WriteEndRequest) (*pb.WriteEndResponse, error) {
	// Update node capacity
	log.Printf("Received Write End request for file '%s'.\n", req.GetFileName())

	writeEndResponse := &pb.WriteEndResponse{}

	filename := req.GetFileName()
	requestID := req.GetRequestId()
	fileSize := req.GetFileSize()
	commit := req.GetCommit()
	cancelWriteRegistration := req.GetCancelWriteRegistration()

	// look up all pending write transactions by file name
	var pwrRequestID WriteTransaction
	var priorPwrOfTheFile WriteTransaction
	var preferenceNodes []string

	// if not master node, commit the transaction by delete temp file and update local replica database
	if !isMasterNode() {
		commitLocalWriteTransaction(filename, requestID, commit)

		// send response to client
		log.Printf("Send Write End response for file '%s'.\n", filename)
		return writeEndResponse, nil
	}

	var result bool

	// look up the pending write transaction and its prior transactions for the file
	for _, tran := range pendingWriteTransactions {
		if tran.FileName == filename {
			if tran.RequestID == requestID {
				pwrRequestID = tran
			} else if tran.FileVersion > priorPwrOfTheFile.FileVersion && priorPwrOfTheFile.FileVersion < pwrRequestID.FileVersion {
				priorPwrOfTheFile = tran
			}
		}
	}

	//if no pending transaction of the requestID, abort the transaction
	if pwrRequestID.FileName == "" {
		// lookup preference nodes of this file
		fileMetadata := lookupFileMetadata(filename)

		if fileMetadata.FileName == "" {
			log.Printf("Warning: The file '%s' is not found on any replica.\n", filename)
			return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Could not find appropriate transaction for file: %v", filename))
		}

		// sending abort request to existing replica nodes of that file
		preferenceNodes = fileMetadata.PreferenceNodes

		result = sendWriteCommitToReplicas(filename, int32(0), requestID, false, preferenceNodes)
		if !result {
			return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Unable to write transaction for file %v.", filename))
		}
		return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Aborted Write transaction for file %v.", filename))
	}

	// If master node, send write end request to the preference nodes to finish up all pending write transactions
	// Send commit to all the replica and wait until receiving the according write quorum acks
	if !cancelWriteRegistration {
		preferenceNodes = pwrRequestID.PreferenceNodes
		priorTransactionIsCommitted := true
		if priorPwrOfTheFile.FileName != "" {
			// if prior pending write transaction,
			// wait until the prior write request is committed or timeout
			priorTransactionIsCommitted = checkIfPriorTransactionIsCommitted(priorPwrOfTheFile.RequestID)
		}

		// abort the write transaction if prior ones are not committed
		if !priorTransactionIsCommitted {
			log.Printf("Prior write transaction for this file has not completed and reaches timeout. Force aborting the prior transaction of request '%s'. \n", priorPwrOfTheFile.RequestID)
			// only master node is able to force aborting pending write transaction to the other nodes.
			forceAbortingWriteTransaction(priorPwrOfTheFile)
			// Otherwise, the current transaction will be aborted
			return writeEndResponse, status.Errorf(codes.Internal, fmt.Sprintf("Unable to finish writing transaction for file %v because the prior transaction is not committed yet", filename))
		}

		sendWriteCommitToReplicas(filename, fileSize, requestID, commit, preferenceNodes)
		updateFileMetadata(pwrRequestID, fileSize)
	}
	removePendingWriteTransaction(requestID)
	server.Backup()

	log.Printf("Sent Write End response for file '%s'.\n", filename)
	return writeEndResponse, nil
}

// force aborting write transaction by removing it from pending write transaction and sending write end request to the other nodes
func forceAbortingWriteTransaction(priorPwrOfTheFile WriteTransaction) {
	removePendingWriteTransaction(priorPwrOfTheFile.RequestID)
	sendWriteCommitToReplicas(priorPwrOfTheFile.FileName, 0, priorPwrOfTheFile.RequestID, false, priorPwrOfTheFile.PreferenceNodes)
}

// Handle read request by streaming the replicated file to client. This API will be processed on any replica nodes
func (*grpcFileServer) Read(req *pb.ReadRequest, stream pb.FileService_ReadServer) error {
	filename := req.GetFileName()
	log.Printf("Received Read request for file %s.\n", filename)

	// Look up its replica files
	var foundReplica FileMetadata
	for _, replica := range replicaFiles {
		if replica.FileName == filename && replica.FileVersion == req.GetFileVersion() {
			foundReplica = replica
			break
		}
	}
	if foundReplica.FileName == "" {
		err := status.Errorf(codes.Internal, "Unable to read file %s since it is not found on the local storage", filename)
		return err
	}

	file := fileHandler.OpenFile(fileHandler.FilePath(filename))
	if file == nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("Could not open file: %v", filename))
	}

	defer file.Close()

	// Read file in a buf of chunk size and stream it back to client
	buf := make([]byte, config.FileChunkSize)

	writing := true
	for writing {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		stream.Send(
			&pb.ReadResponse{
				FileValue: buf[:n],
			})
	}
	return nil
}

// Acquire the highest version of file and the replica nodes that are currently storing the replicas of that file
func (*grpcFileServer) ReadAcquire(ctx context.Context, req *pb.ReadAcquireRequest) (*pb.ReadAcquireResponse, error) {
	filename := req.GetFileName()
	log.Printf("Receive Read Acquire request for file '%s'.\n", filename)

	fileMetadata := lookupFileMetadata(filename)

	readAcquireResponse := &pb.ReadAcquireResponse{
		Status: false,
	}

	if fileMetadata.FileName == "" {
		log.Printf("Could not find the file %s on any replica.\n", filename)
		err := status.Errorf(codes.Internal, "Unable to acquire read for file %s: Not found on any replica", filename)
		return readAcquireResponse, err
	}

	// if not master node, return file metadata on its local storage
	if !isMasterNode() {
		for _, replicatedFile := range replicaFiles {
			if replicatedFile.FileName == filename {
				readAcquireResponse.Status = true
				readAcquireResponse.FileVersion = replicatedFile.FileVersion
				break
			}
		}
		if readAcquireResponse.Status {
			log.Printf("Sent Read Acquire response for file '%s': version '%d'\n", filename, readAcquireResponse.GetFileVersion())
		} else {
			log.Printf("Sent Read Acquire response for file '%s'. Not found", filename)
		}

		return readAcquireResponse, nil
	}

	var highestFileVersion int32
	var preferencesNodes []string

	highestFileVersion = -1
	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan readAcquireResult)
	for _, preferenceNode := range fileMetadata.PreferenceNodes {
		// if preferenceNode is the master, return the file version on its replica list
		if preferenceNode == masterNode {
			foundReplica := false
			for _, replica := range replicaFiles {
				if replica.FileName == fileMetadata.FileName {
					if replica.FileVersion > highestFileVersion {
						highestFileVersion = replica.FileVersion
						preferencesNodes = append(preferencesNodes, masterNode)
					}
					foundReplica = true
					break
				}
			}
			if foundReplica {
				numberOfAcksReceived++
			} else {
				log.Printf("Warning: Unable to find a replica of file %s on node %s", filename, preferenceNode)
			}
			continue
		}
		numberOfWaitingAcks++
		go sendReadAcquireToReplica(filename, preferenceNode, c)
	}

	for i := 0; i < numberOfWaitingAcks; i++ {
		raResult := <-c
		if raResult.status {

			numberOfAcksReceived++
			if raResult.version >= highestFileVersion {
				highestFileVersion = raResult.version
				preferencesNodes = append(preferencesNodes, raResult.targetNode)
			}
		}
		if reachWriteQuorum(numberOfAcksReceived) {
			readAcquireResponse.FileVersion = highestFileVersion
			readAcquireResponse.PreferenceNodes = preferencesNodes
			readAcquireResponse.Status = true
			log.Printf("Sent Read Acquire response for file '%s'. Found with version '%d' on the following node(s): '%s'\n", filename, highestFileVersion, preferencesNodes)

			return readAcquireResponse, nil
		}
	}

	err := status.Errorf(codes.Internal, "Error: Unable to acquire read for file %s because not receiving enough ack from the replicas", filename)

	return readAcquireResponse, err
}

// Remove file, which will be handled on the master node
func (*grpcFileServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	filename := req.GetFileName()
	log.Printf("Receive Delete file %s request.\n", filename)

	deleteResponse := &pb.DeleteResponse{
		Status: true,
	}

	// if not master node, delete the file and return the ack
	if !isMasterNode() {
		fileHandler.Delete(fileHandler.FilePath(filename))
		//update local replica list
		removeFileOnLocalReplicatedFiles(filename)
		return deleteResponse, nil
	}

	fileMetadata := lookupFileMetadata(filename)

	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan bool)
	preferenceNodes := fileMetadata.PreferenceNodes
	for _, preferenceNode := range preferenceNodes {
		// if preferenceNode is the master, delete the file and incerase the number of ack
		if preferenceNode == masterNode {
			fileHandler.Delete(fileHandler.Fs533FilePath(filename))
			numberOfAcksReceived++
			continue
		}
		numberOfWaitingAcks++
		go sendDeleteRequestToReplica(filename, preferenceNode, c)
	}

	for i := 0; i < numberOfWaitingAcks; i++ {
		ackReceived := <-c
		if ackReceived {
			numberOfAcksReceived++
		}
		if numberOfAcksReceived >= config.WriteQuorum {
			//update file system
			removeFileOnFS(filename)

			server.Backup()
			return deleteResponse, nil
		}
	}

	deleteResponse.Status = false
	err := status.Errorf(codes.Internal, "Error: Unable to delete file because not receiving enough ack from the replicas.")
	return deleteResponse, err
}

// TODO: REFACTOR
// ------------------------------------------------PRIVATE FUNCTIONS------------------------------------------------

// send voting requests to other nodes to promote itself as a new master
func sendVotingRequests() bool {
	// if this node already voted for another node to be the leader, it should not vote for itself in this election term

	alreadyVoted := false
	for _, voteRecord := range voteRecords {
		if voteRecord == electionTerm {
			alreadyVoted = true
		}
	}
	if alreadyVoted && onElectionProcess && membershipService.Len() == 2 {
		return false
	}

	numWaitingAcks := 0
	c := make(chan bool)
	for _, member := range membershipService.GetFollowerAddresses() {
		if member == config.IPAddress {
			continue
		}
		numWaitingAcks++
		go sendVotingRequest(member, c)
	}

	numAcceptance := 0
	for i := 0; i < numWaitingAcks; i++ {
		r := <-c
		if r {
			numAcceptance++
		}
		if numAcceptance > countTheMajorityNumber(numWaitingAcks) || (numAcceptance == 1 && membershipService.Len() == 2) {
			return true
		}
	}
	return false
}

// send vote request to the tartget node
func sendVotingRequest(targetNode string, cResult chan bool) {
	log.Printf("Send Vote request to node %s to promote %s as new master.\n", targetNode, config.IPAddress)
	votingRequest := &pb.VotingRequest{
		ElectionTerm:     electionTerm,
		CandidateAddress: config.IPAddress,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", targetNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to connect to target node at %s. Error: %v\n", targetNode, err.Error())
		log.Printf("Unable to send promote request to node '%s'. Error: %s\n", targetNode, errorMessage)
		cResult <- false
		return
	}

	defer cc.Close()

	// timeout mechanism
	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	_, err1 := c.Vote(ctx, votingRequest)

	if err1 != nil {
		log.Println("Error: Unable to send promote request to node.", targetNode, "Error: ", err)
		cResult <- false
		return
	}
	cResult <- true
}

// count majority votes
func countTheMajorityNumber(memberCount int) int {
	return int(math.Ceil(float64(memberCount) / float64(2)))
}

// send announcement about the new master to the other nodes
func sendAnnouncementRequests() {
	log.Printf("Send Announcement request to all members about new master.\n")
	announcementRequest := &pb.AnnouncementRequest{
		NewMasterAddress: config.IPAddress,
	}

	for _, member := range membershipService.GetFollowerAddresses() {
		if member == config.IPAddress {
			continue
		}

		cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", member, config.TCPPort), grpc.WithInsecure())
		if err != nil {
			errorMessage := fmt.Sprintf("Unable to connect to target node at %s. Error: %v\n", member, err.Error())
			log.Printf("Unable to send promote request to node '%s'. Error: %s\n", member, errorMessage)
			continue
		}

		defer cc.Close()

		// timeout
		duration := time.Duration(config.Timeout) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		c := pb.NewFileServiceClient(cc)

		_, err1 := c.Announce(ctx, announcementRequest)

		if err1 != nil {
			log.Println("Error: Unable to send promote request to node.", member, "Error: ", err)
		}
	}
}

// send request to a node storing the replica of the file to clone it to the target node
func sendBackupFileRequest(relocatedFile relocatedFileMetadata, ackChan chan bool) {
	log.Printf("Send Backup File request to replicate file '%s' from node '%s' to node '%s'.\n", relocatedFile.fileName, relocatedFile.srcReplicaNode, relocatedFile.destReplicaNode)

	//gRPC call
	backupFileRequest := &pb.BackupFileRequest{
		TargetNode:  relocatedFile.destReplicaNode,
		FileName:    relocatedFile.fileName,
		FileVersion: relocatedFile.fileVersion,
		FileSize:    relocatedFile.fileSize,
	}
	requestSize := proto.Size(backupFileRequest)
	log.Println("Sent Backup request to '", relocatedFile.srcReplicaNode, "' with size: ", requestSize)

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", relocatedFile.srcReplicaNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to connect to target node at %s. Error: %v\n", relocatedFile.srcReplicaNode, err.Error())
		log.Printf("Unable to send backup to node '%s'. Error: %s\n", relocatedFile.srcReplicaNode, errorMessage)
		return
	}

	defer cc.Close()

	// timeout
	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	backupFileResponse, err1 := c.BackupFile(ctx, backupFileRequest)

	responseSize := proto.Size(backupFileResponse)
	log.Println("Sent Backup Response to '", relocatedFile.srcReplicaNode, "'with size: ", responseSize)

	if err1 != nil {
		log.Printf("Error: Unable to backup file '%s' from node '%s' to node '%s'. Error '%s'.\n", relocatedFile.fileName, relocatedFile.srcReplicaNode, relocatedFile.destReplicaNode, err1)
		ackChan <- false
		return
	}

	ackChan <- true
	log.Printf("End Backup request for file '%s' from node '%s' to node %s.\n", relocatedFile.fileName, relocatedFile.srcReplicaNode, relocatedFile.destReplicaNode)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// send request to all the node to promote the new master node
func sendPromoteRequest(newMasterNode string, targetNode string) {
	log.Printf("Send Promote request to node %s to promote %s as new master.\n", targetNode, newMasterNode)
	promoteRequest := &pb.PromoteRequest{
		NewMasterNode: newMasterNode,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", targetNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to connect to target node at %s. Error: %v\n", targetNode, err.Error())
		log.Printf("Unable to promote request to node '%s'. Error: %s\n", targetNode, errorMessage)
		return
	}

	defer cc.Close()

	// timeout
	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	_, err1 := c.Promote(ctx, promoteRequest)

	if err1 != nil {
		log.Println("Unable to send promote request to node ", targetNode, ". Error: ", err)
	}

	log.Printf("Done sending Promote request to node %s to promote %s as new master.\n", targetNode, newMasterNode)
}

// Wait and check if the prio write transaction is committed
func checkIfPriorTransactionIsCommitted(requestID string) bool {
	timeout := time.After(time.Duration(config.PendingWriteTransactionTimeoutInSeconds) * time.Second)
	tick := time.Tick(500 * time.Millisecond)
	// Retry until the timeout
	for {
		select {
		case <-timeout:
			return false

		case <-tick:
			committed := true
			for _, tran := range pendingWriteTransactions {
				if tran.RequestID == requestID {
					committed = false
				}
			}
			if committed {
				return true
			}
		}
	}
}

// clean up temp file created on write transaction, deleted after rename to the filename
// if committed, the transaction is successful and the local replica files are also updated updated
func commitLocalWriteTransaction(filename string, requestID string, commit bool) {
	tempFileIndex := -1
	var fileSize int32
	var fileVersion int32
	var tempFile string

	for i := 0; i < len(temporaryFiles); i++ {
		if temporaryFiles[i].RequestID == requestID {
			tempFileIndex = i
			fileSize = temporaryFiles[i].FileSize
			fileVersion = temporaryFiles[i].FileVersion
			tempFile = temporaryFiles[i].TempFileName
			break
		}
	}

	if commit {
		if fileVersion > 1 {
			for i := 0; i < len(replicaFiles); i++ {
				if replicaFiles[i].FileName == filename {
					if fileVersion > replicaFiles[i].FileVersion {
						// update replica with higher version
						replicaFiles[i].FileSize = fileSize
						replicaFiles[i].FileVersion = fileVersion
					}
					break
				}
			}
		} else {
			newReplica := FileMetadata{
				FileName:    filename,
				FileSize:    fileSize,
				FileVersion: fileVersion,
			}
			replicaFiles = append(replicaFiles, newReplica)
		}
	}

	if tempFileIndex != -1 {
		temporaryFiles = append(temporaryFiles[:tempFileIndex], temporaryFiles[tempFileIndex+1:]...)
		log.Printf("Deleted temporary file '%s' for file '%s'", tempFile, filename)
		fileHandler.Copy(fileHandler.FilePath(tempFile), fileHandler.FilePath(filename))
		fileHandler.Delete(fileHandler.FilePath(tempFile))
	} else {
		log.Printf("Warning: Unable to find temporary file for file '%s'", filename)
	}
}

// remove pending write transaction by requestID
func removePendingWriteTransaction(requestID string) {
	pendingIndex := -1

	for i := 0; i < len(pendingWriteTransactions); i++ {
		if pendingWriteTransactions[i].RequestID == requestID {
			pendingIndex = i
			break
		}
	}
	pendingWriteTransactions = append(pendingWriteTransactions[:pendingIndex], pendingWriteTransactions[pendingIndex+1:]...)
}

// send write transaction commit request to replica nodes.
func sendWriteCommitToReplicas(filename string, filesize int32, requestID string, commit bool, preferenceNodes []string) bool {
	log.Printf("Sent Write End request to all perference nodes '%s'.\n", preferenceNodes)

	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan bool)
	for _, preferenceNode := range preferenceNodes {
		// return the file version on its replica list if preference node is the master
		if preferenceNode == masterNode {
			commitLocalWriteTransaction(filename, requestID, commit)
			numberOfAcksReceived++
			continue
		}
		log.Printf("send Write End request to node '%s'.\n", preferenceNode)

		numberOfWaitingAcks++
		go sendWriteCommitToReplica(requestID, filename, filesize, preferenceNode, commit, c)
	}

	for i := 0; i < numberOfWaitingAcks; i++ {
		raResult := <-c
		if raResult {
			numberOfAcksReceived++
		}

		if reachWriteQuorum(numberOfAcksReceived) {
			return true
		}
	}
	return false
}

// send end write commit request to the replica node
func sendWriteCommitToReplica(requestID string, filename string, filesize int32, preferenceNode string, commit bool, ackChan chan bool) {
	ackChan <- client.InternalEndWriteFile(requestID, filename, filesize, preferenceNode, false, commit)
}

// regenerate nodes
func updateNodes() {
	var updatedNodes []nodeMetadata

	for _, fileMetadata := range files {
		for _, node := range fileMetadata.PreferenceNodes {
			isAdded := false
			for idx, node2 := range updatedNodes {
				if node2.NodeAddress == node {
					isAdded = true
					updatedNodes[idx].TotalFileSize = updatedNodes[idx].TotalFileSize + fileMetadata.FileSize
					updatedNodes[idx].Files = append(updatedNodes[idx].Files, fileMetadata.FileName)
				}
			}
			if !isAdded {
				anodemt := nodeMetadata{
					NodeAddress:   node,
					TotalFileSize: fileMetadata.FileSize,
					Files:         []string{fileMetadata.FileName},
				}
				updatedNodes = append(updatedNodes, anodemt)
			}
		}
	}

	for _, member := range membershipService.GetNodeAddresses() {
		alreadyAdded := false
		for _, node := range updatedNodes {
			if member == node.NodeAddress {
				alreadyAdded = true
				break
			}
		}
		if !alreadyAdded {
			anodemt := nodeMetadata{
				NodeAddress:   member,
				TotalFileSize: 0,
				Files:         []string{},
			}
			updatedNodes = append(updatedNodes, anodemt)
		}
	}

	nodes = updatedNodes
}

// remove file on local replicated files, managed by replica node
func removeFileOnLocalReplicatedFiles(filename string) {
	fileIndex := -1

	for i := 0; i < len(replicaFiles); i++ {
		if replicaFiles[i].FileName == filename {
			fileIndex = i
			break
		}
	}

	replicaFiles = append(replicaFiles[:fileIndex], replicaFiles[fileIndex+1:]...)
}

// remove file on global files, managed by master nonde
func removeFileOnFS(filename string) {
	fileIndex := -1

	for i := 0; i < len(files); i++ {
		if files[i].FileName == filename {
			fileIndex = i
			break
		}
	}

	files = append(files[:fileIndex], files[fileIndex+1:]...)
	updateNodes()
}

// allocate new replica to the first N nodes with highest capacity
func allowcatePreferenceNodes() []string {
	var result []string
	if len(nodes) <= config.NumberOfReplicas {
		for _, node := range nodes {
			result = append(result, node.NodeAddress)
		}
		return result
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].TotalFileSize < nodes[j].TotalFileSize
	})

	for i := 0; i < config.NumberOfReplicas; i++ {
		result = append(result, nodes[i].NodeAddress)
	}

	return result
}

// send read acquire request to the replica node storing the file being read
// return status, file version and preference nodes
func sendReadAcquireToReplica(filename string, replicaNodeAddress string, ackChan chan readAcquireResult) {
	r1, r2, _ := client.InternalReadAcquireFile(filename, replicaNodeAddress)
	result := readAcquireResult{
		status:     r1,
		version:    int32(r2),
		targetNode: replicaNodeAddress,
	}
	ackChan <- result
}

// send delete request to the replica node storing the file being removed
func sendDeleteRequestToReplica(filename string, replicaNodeAddress string, ackChan chan bool) {
	ackChan <- client.InternalRemoveFile(filename, replicaNodeAddress)
}

// verify if request reaches write quorum, varies based on the number of active nodes.
// Less than 3 nodes, the consensus reaches only if the number of acks is greater equal to the number of active nodes
// Otherwise, the consensus reaches if the number of acks is greater or equal the number of write quorum
func reachWriteQuorum(numberOfAcksReceived int) bool {
	if membershipService.Len() < 3 {
		return numberOfAcksReceived >= membershipService.Len()
	}
	return numberOfAcksReceived >= config.WriteQuorum
}

// update file metadata
func updateFileMetadata(writeTransaction WriteTransaction, fileSize int32) {
	updated := false
	for i := 0; i < len(files); i++ {
		if files[i].FileName == writeTransaction.FileName {
			updated = true
			files[i].FileVersion = writeTransaction.FileVersion
			files[i].FileSize = fileSize
			files[i].RegisteredTime = writeTransaction.RegisteredTime

			break
		}
	}

	if !updated {
		newFileMetadata := FileMetadata{
			FileName:        writeTransaction.FileName,
			FileSize:        fileSize,
			FileVersion:     writeTransaction.FileVersion,
			PreferenceNodes: writeTransaction.PreferenceNodes,
			RegisteredTime:  writeTransaction.RegisteredTime,
		}
		files = append(files, newFileMetadata)
	}

	updateNodes()
}

// lookup file metadata by its name on master node
func lookupFileMetadata(filename string) FileMetadata {
	for _, file := range files {
		if filename == file.FileName {
			return file
		}
	}
	return FileMetadata{
		FileName:        "",
		FileVersion:     0,
		FileSize:        0,
		PreferenceNodes: []string{},
	}
}

// check if the node handling request is for master node. Different processes for master node and follower nodes
func isMasterNode() bool {
	if masterNode == config.IPAddress {
		return true
	}
	return false
}
