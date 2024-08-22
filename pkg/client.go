package pkg

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nathang15/go-fs/pb"
	"google.golang.org/grpc"
)

type Client struct{}

type FetchedFileMetadata struct {
	FileName      string
	FileVersion   int32
	FileSize      int32
	LocalFileName string
}

var replicaFiles []FileMetadata
var fetchedFiles []FetchedFileMetadata
var fileHandler Operation
var logManager Logger

func (client *Client) Put(localfilename string, filename string) {
	log.Printf("COMMAND PUT file '%s' to file '%s' to master node at '%s'.\n", localfilename, filename, masterNode)

	startWriteTime := time.Now()

	requestID := uuid.New().String()
	result, needUserInteraction, fileVersion, preferenceNodes := client.InternalRegisterWriteFile(requestID, filename, masterNode)
	if !result {
		log.Printf("Error: unable to put local file %s as fs file %s.\n", localfilename, filename)
		return
	}

	if needUserInteraction {
		log.Println("Wait time < 1 minute. Press y to continue, n to cancel.")
		logManager.DisableLog()

		cancelRequest := false

		c := make(chan string, 1)
		go scan(c)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		select {
		case <-ctx.Done():
			cancelRequest = true
			logManager.EnableLog()
		case userDecision := <-c:
			logManager.EnableLog()
			if !strings.EqualFold(userDecision, "yes") && !strings.EqualFold(userDecision, "y") {
				cancelRequest = true
			}
		}

		if cancelRequest {
			client.InternalEndWriteFile(requestID, filename, 0, masterNode, true, false)
			log.Printf("Undo PUT cmd file %s due to timeout.\n", filename)
			return
		}
	}

	log.Printf("Send prep write request to upload local file '%s' to replica nodes at '%s'.\n", localfilename, preferenceNodes)

	numberOfAcksReceived := 0
	numberOfWaitingAcks := 0
	c := make(chan int32)

	var fileSize int32
	for _, preferenceNode := range preferenceNodes {
		log.Printf("sending prep write request to preference node '%s'", preferenceNode)

		numberOfWaitingAcks++
		go sendWritePreparationToReplica(requestID, localfilename, filename, fileVersion, preferenceNode, c)
	}

	log.Printf("waiting for '%d' ack from preference nodes \n", numberOfWaitingAcks)

	reachWriteQuorumFlag := false
	for i := 0; i < numberOfWaitingAcks; i++ {
		fileSize = <-c
		if fileSize > 0 {
			numberOfAcksReceived++
		}
		if reachWriteQuorum(numberOfAcksReceived) {
			reachWriteQuorumFlag = true
			break
		}
	}

	if reachWriteQuorumFlag {
		log.Printf("Send commit request to master to finish write request since it already reaches write quorum")
		result := client.InternalEndWriteFile(requestID, filename, fileSize, masterNode, false, true)
		if result {
			reportWriteTime(filename, fileVersion, fileSize, startWriteTime, time.Now())
			log.Printf("FINISH COMMAND PUT file %s, new version is %d, replicas are on nodes: %s.\n", filename, fileVersion, preferenceNodes)
		} else {
			log.Printf("ROLLBACK COMMAND PUT file %s, it is unable to put file to fs533 system due to some reasons.\n", filename)
		}
	} else {
		client.InternalEndWriteFile(requestID, filename, fileSize, masterNode, false, false)
		log.Printf("Rollback PUT for file %s.\n", filename)
	}
}

func (client *Client) Fetch(filename string, localfilename string) {
	startReadTime := time.Now()
	log.Printf("Fetching for file '%s': request sent to master node at '%s'.\n", filename, masterNode)

	log.Printf("Send read request for file '%s'. Request sent to master node at at '%s'.\n", filename, masterNode)

	result, fileVersion, preferenceNodes := client.InternalReadAcquireFile(filename, masterNode)
	if !result {
		log.Printf("Finish fetching file %s, unable to fetch to local disk as file %s.\n", filename, localfilename)
		return
	}

	if len(preferenceNodes) == 0 {
		log.Printf("File '%s' is not found on any replica.\n", filename)
	} else {
		log.Printf("Receive read response for file %s. File has version '%d' and is available at '%s'.\n", filename, fileVersion, preferenceNodes)
	}

	fetchedFileExisted, fetchedFileSize := lookupLocalFetchedFile(filename, fileVersion, localfilename)
	if fetchedFileExisted {
		log.Printf("File '%s' already cached on local disk, it is fetch to local disk as file %s.\n", filename, localfilename)
		reportReadTime(filename, fileVersion, fetchedFileSize, startReadTime, time.Now())
		return
	}

	replicatedFileExisted, replicatedFileSize := lookupReplicatedFile(filename, preferenceNodes, fileVersion, localfilename)

	if replicatedFileExisted {
		log.Printf("File '%s' already has a replica on local disk, it is fetch to local disk as file %s.\n", filename, localfilename)
		reportReadTime(filename, fileVersion, replicatedFileSize, startReadTime, time.Now())
		return
	}

	log.Printf("Send read request for file %s from node '%s' to replica node at '%s'.\n", filename, config.IPAddress, masterNode)

	var fileSize int32
	for _, replicaNode := range preferenceNodes {
		fileSize = readFileFromReplica(filename, localfilename, fileVersion, replicaNode)
		if fileSize > 0 {
			log.Printf("Finished fetching file %s, to local disk as %s.\n", filename, localfilename)
			reportReadTime(filename, fileVersion, fileSize, startReadTime, time.Now())
			break
		}

	}

	log.Printf("Finish fetching file %s, unable to fetch to local disk as file %s.\n", filename, localfilename)
}

// List all the local fetched files, local replicated files and global files metadata
func (client *Client) Report() {
	log.Println("----------------------------------REPORT-------------------------------------")
	if masterNode == config.IPAddress {
		log.Println("This is MASTER!")
	} else {
		log.Println("Master node is at ", masterNode)
	}

	log.Printf("Election term = '%d'.\n", electionTerm)

	log.Printf("All replica nodes for file systems are listed below.\n")
	for idx, node := range nodes {
		log.Printf("Node %d: '%s', total size '%d', storing replicated files %s.\n", idx, node.NodeAddress, node.TotalFileSize, node.Files)
	}

	log.Printf("All files on fs533 systems are listed below.\n")
	for idx, fileMetada := range files {
		log.Printf("File %d: %s, version %d, replicated on %s.\n", idx, fileMetada.FileName, fileMetada.FileVersion, fileMetada.PreferenceNodes)
	}

	log.Printf("All local replicated files are listed below.\n")
	for idx, replicatedFileMetada := range replicaFiles {
		log.Printf("File %d: %s, version %d.\n", idx, replicatedFileMetada.FileName, replicatedFileMetada.FileVersion)
	}

	log.Printf("All local fetched files are listed below.\n")
	for idx, fetchedFileMetada := range fetchedFiles {
		log.Printf("File %d: %s, version %d, fetched as file %s.\n", idx, fetchedFileMetada.FileName, fetchedFileMetada.FileVersion, fetchedFileMetada.LocalFileName)
	}

	log.Println("----------------------------------END REPORT-------------------------------------")
}

// List all files on the system
func (client *Client) ListHere() {
	log.Printf("LSHERE command\n")

	log.Printf("All local replicated files on folder %s are: \n", config.FilePath)
	filenames := fileHandler.ListFilesInFolder(config.FilePath)
	for idx, file := range filenames {
		log.Printf("File %d: %s\n", idx, file)
	}

	log.Printf("All local files on folder %s are: \n", config.LocalFilePath)
	localFilenames := fileHandler.ListFilesInFolder(config.LocalFilePath)
	for idx, file := range localFilenames {
		log.Printf("File %d: %s\n", idx, file)
	}
}

// delete file
func (client *Client) RemoveFile(filename string) {
	log.Printf("REMOVE %s: request sent from node '%s' to master node at '%s'.\n", filename, config.IPAddress, masterNode)

	client.InternalRemoveFile(filename, masterNode)

	log.Printf("REMOVE %s: Successfully removed file.\n", filename)
}

// List all machines (name / id / IP address) on the servers that contain a copy of the file.
func (Client *Client) PrintLocateFile(filename string) {
	locatedFile := client.LocateFile(filename)

	if locatedFile.FileName == "" {
		log.Printf("Warning: the file '%s' is not found on system", filename)
		return
	}

	log.Printf("File '%s' is stored on the following nodes \n", filename)
	for idx, replica := range locatedFile.PreferenceNodes {
		log.Printf("Node %d: %s\n", idx, replica)
	}
}

// Locate replica node for file
func (client *Client) LocateFile(filename string) FileMetadata {
	log.Printf("LOCATE for file '%s': request sent to master node at '%s'.\n", filename, masterNode)

	//gRPC call
	locateRequest := &pb.LocateRequest{
		FileName: filename,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", masterNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s because: %v\n", masterNode, err.Error())
		log.Printf("Cannot query all files. Error: %s\n", errorMessage)
		return FileMetadata{}
	}

	defer cc.Close()

	// Timeout mechanism
	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	res, err1 := c.Locate(ctx, locateRequest)

	if err1 != nil {
		log.Println("Error: Unable to read the response for listall command. ", err1)
		return FileMetadata{}
	}

	replicas := []string{}
	for _, replica := range res.GetReplicas() {
		replicas = append(replicas, replica.GetNodebyAddress())
	}

	returnValue := FileMetadata{
		FileName:        res.GetFileName(),
		FileSize:        res.GetFileSize(),
		FileVersion:     res.GetFileVersion(),
		PreferenceNodes: replicas,
	}
	return returnValue
}

// List all files
func (client *Client) ListAllFiles() []string {
	log.Printf("LIST ALL request sent to master node at '%s'.\n", masterNode)

	listAllRequest := &pb.Empty{}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", masterNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to conntect to target node at %s. %v\n", masterNode, err.Error())
		log.Printf("Cannot query all files. Error: %s\n", errorMessage)
		return []string{}
	}

	defer cc.Close()

	// Timeout mechanism
	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	res, err1 := c.ListAll(ctx, listAllRequest)

	if err1 != nil {
		log.Println("Error: It is unable read the response for listall command due to error ", err)
		return []string{}
	}

	allfiles := []string{}

	for _, file := range res.GetFiles() {
		fileMetadata := FileMetadata{
			FileName:    file.GetFileName(),
			FileSize:    file.GetFileSize(),
			FileVersion: file.GetFileVersion(),
		}
		allfiles = append(allfiles, fileMetadata.FileName)
	}
	return allfiles
}

// Delete all local test files
func (client *Client) ClearLocalDbTestFiles() {
	fileHandler.ClearLocalDB("test_")
}

// Delete all files
func (client *Client) DeleteAllFiles() {
	log.Printf("DELETE ALL FILES request sent to master node at '%s'.\n", masterNode)

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", masterNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to connect to target node at %s because: %v\n", masterNode, err.Error())
		log.Printf("Cannot query all files. Error: %s\n", errorMessage)
	}

	defer cc.Close()

	// timeout mechanism
	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	_, err1 := c.DeleteAll(ctx, &pb.Empty{})

	if err1 != nil {
		log.Println("Error: It is unable read the response for DeleteAllFiles command due to error ", err1)
	}
}

// /Print all files queried from ListAllFile API
func (client *Client) PrintAllFiles(files []string) {
	if len(files) == 0 {
		log.Printf("File system empty!\n")
		return
	}

	log.Printf("There are %d files on the system.\n", len(files))
	for idx, file := range files {
		log.Printf("File %d: name = %s \n", idx, file)
	}
}

func (c *Client) Init(master string, loadedConfig Config) {
	masterNode = master
	config = loadedConfig
}

// TODO: REFACTOR LATER
// ----------------INTERNAL FUNCTIONS----------------
// Finish write transaction
func (client *Client) InternalEndWriteFile(requestID string, filename string, filesize int32, targetNodeAddress string, cancelWriteRegistration bool, commit bool) bool {
	//gRPC call
	writeEndRequest := &pb.WriteEndRequest{
		RequestId:               requestID,
		FileName:                filename,
		FileSize:                filesize,
		CancelWriteRegistration: cancelWriteRegistration,
		Commit:                  commit,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", targetNodeAddress, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s. Error: %v\n", targetNodeAddress, err.Error())
		log.Printf("Cannot end write request for file '%s'. Error: %s\n", filename, errorMessage)
		return false
	}

	defer cc.Close()

	//timeout mechanism
	duration := time.Duration(config.Timeout*100) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	_, endErr := c.WriteEnd(ctx, writeEndRequest)

	if endErr != nil {
		log.Println("Unable to end the write transaction for file ", filename, " due to error ", endErr)
		return false
	}

	return true
}

// Register write operation to master node
func (client *Client) InternalRegisterWriteFile(requestID string, filename string, targetNodeAddress string) (bool, bool, int32, []string) {
	log.Printf("Send register write request for file %s to master node at '%s'.\n", filename, masterNode)

	//gRPC call
	writeRegisterRequest := &pb.WriteRegisterRequest{
		FileName:  filename,
		RequestId: requestID,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", targetNodeAddress, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s. Error: %v\n", targetNodeAddress, err.Error())
		log.Printf("Cannot register write request for file '%s'. Error: %s\n", filename, errorMessage)
		return false, false, -1, nil
	}

	defer cc.Close()

	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	writeRegisterResponse, err := c.WriteRegister(ctx, writeRegisterRequest)

	if err != nil {
		log.Println("Unable to register write operation for file ", filename, " due to error ", err)
		return false, false, -1, nil
	}

	log.Printf("Received register write response for file %s: version %d, preference nodes are '%s', user interaction: %v.\n", filename, writeRegisterResponse.GetFileVersion(), writeRegisterResponse.GetPreferenceNodes(), writeRegisterResponse.GetNeedUserInteraction())

	return true, writeRegisterResponse.GetNeedUserInteraction(), writeRegisterResponse.GetFileVersion(), writeRegisterResponse.GetPreferenceNodes()
}

// Send read acquire request to the target node to get the file's latest version on the target replica node and its preference nodes
func (client *Client) InternalReadAcquireFile(filename string, replicaNodeAddress string) (bool, int32, []string) {
	//gRPC call
	readAcquireRequest := &pb.ReadAcquireRequest{
		SenderAddress: config.IPAddress,
		FileName:      filename,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", replicaNodeAddress, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("could not connect to target node at %s. Error: %v\n", replicaNodeAddress, err.Error())
		log.Printf("Cannot acquire read for file '%s'. Error: %s\n", filename, errorMessage)
		return false, -1, nil
	}

	defer cc.Close()

	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	readAcquireResponse, err := c.ReadAcquire(ctx, readAcquireRequest)

	if err != nil {
		log.Println("Unable to acquire read file ", filename, " due to error ", err)
		return false, -1, nil
	}

	if !readAcquireResponse.Status {
		log.Println("Unable to acquire read file  ", filename)
		return false, -1, nil
	}

	return true, readAcquireResponse.GetFileVersion(), readAcquireResponse.GetPreferenceNodes()
}

// Send remove request directly to the target node
func (client *Client) InternalRemoveFile(filename string, targetNode string) bool {
	//gRPC call
	removeRequest := &pb.DeleteRequest{
		SenderAddress: config.IPAddress,
		FileName:      filename,
	}

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", targetNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to connect to node at %s. Error: %v\n", masterNode, err.Error())
		log.Printf("Unable to remove file '%s'. Error: %s\n", filename, errorMessage)
		return false
	}

	defer cc.Close()

	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	removeResponse, err := c.Delete(ctx, removeRequest)

	if err != nil {
		log.Println("Unable to delete file ", filename, " due to error ", err)
		return false
	}

	if !removeResponse.Status {
		log.Println("Unable to delete file ", filename)
		return false
	}

	return true
}

// TODO: REFACTOR LATER
// -------------PRIVATE FUNCTIONS-------------
func reportWriteTime(filename string, fileVersion int32, fileSize int32, startWriteTime time.Time, endWriteTime time.Time) {
	log.Printf("******************* WRITE REPORT **********************")
	log.Printf("Report for file '%s', version '%d', size '%d'", filename, fileVersion, fileSize)
	log.Printf("Start write at '%s'", ConvertTimeToLongString(startWriteTime))
	log.Printf("End write at '%s'", ConvertTimeToLongString(endWriteTime))
	log.Printf("Runtime: '%d' ms", endWriteTime.Sub(startWriteTime).Milliseconds())
	log.Printf("******************* END WRITE REPORT **********************")
}

func reportReadTime(filename string, fileVersion int32, fileSize int32, startWriteTime time.Time, endWriteTime time.Time) {
	log.Printf("******************* READ REPORT **********************")
	log.Printf("Report for file '%s', version '%d', size '%d'", filename, fileVersion, fileSize)
	log.Printf("Start read at '%s'", ConvertTimeToLongString(startWriteTime))
	log.Printf("End read at '%s'", ConvertTimeToLongString(endWriteTime))
	log.Printf("Runtime: '%d' ms", endWriteTime.Sub(startWriteTime).Milliseconds())
	log.Printf("******************* END READ REPORT **********************")
}

// send write preparation to replica node
func sendWritePreparationToReplica(requestID string, localfilename string, filename string, fileVersion int32, preferenceNode string, ackChan chan int32) {
	//if preferenceNode is the current node then clone local file to file system
	if preferenceNode == config.IPAddress {
		log.Printf("Perference node '%s' is the current node.", preferenceNode)
		tempfilename := fmt.Sprintf("___tempfile_%s", uuid.New())
		fs, err := fileHandler.Copy(fileHandler.LocalFilePath(localfilename), fileHandler.FilePath(tempfilename))
		if err == nil {

			tempFile := TempFile{
				FileName:     filename,
				FileVersion:  fileVersion,
				RequestID:    requestID,
				TempFileName: tempfilename,
				FileSize:     int32(fs),
			}
			ackChan <- int32(fs)
			temporaryFiles = append(temporaryFiles, tempFile)
		}
		return
	}

	file := fileHandler.OpenFile(fileHandler.LocalFilePath(localfilename))
	if file == nil {
		log.Println("Could not open local file %s.", localfilename)
		ackChan <- 0
		return
	}

	defer file.Close()

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", preferenceNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Could not connect to target node at %s. Error: %v\n", preferenceNode, err.Error())
		log.Printf("Cannot proceed write preparation request for file '%s'. Error: %s\n", filename, errorMessage)
		ackChan <- 0
		return
	}

	defer cc.Close()

	duration := time.Duration(config.Timeout*100) * time.Second
	_, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	stream, err := c.WritePrepare(context.Background())
	if err != nil {
		log.Printf("Error calling WritePrepare: %v", err)
		ackChan <- 0
		return
	}

	buf := make([]byte, config.FileChunkSize)

	writing := true
	for writing {
		_, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		writePreparationRequest := &pb.WritePreparationRequest{
			FileName:    filename,
			FileValue:   buf,
			FileVersion: fileVersion,
			RequestId:   requestID,
		}
		stream.Send(
			writePreparationRequest)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error receiving response from write preparation request: %v", err)
		ackChan <- 0
		return
	}

	ackChan <- res.GetFileSize()
}

// look up replicated file on the host
func lookupReplicatedFile(filename string, preferenceNodes []string, fileVersion int32, localfilename string) (bool, int32) {
	isReplicaNode := false
	for _, preferenceNode := range preferenceNodes {
		if preferenceNode == config.IPAddress {
			isReplicaNode = true
			break
		}
	}

	if !isReplicaNode {
		return false, 0
	}

	hasReplicaFileOnLocalStorage := false
	hasCorrectReplicaVersionOnLocalStorage := false
	for _, replicatedFile := range replicaFiles {
		if replicatedFile.FileName == filename {
			hasReplicaFileOnLocalStorage = true
			if replicatedFile.FileVersion == int32(fileVersion) {
				hasCorrectReplicaVersionOnLocalStorage = true
			}
			break
		}
	}

	if !hasCorrectReplicaVersionOnLocalStorage {
		if hasReplicaFileOnLocalStorage {
			log.Printf("Warning: Replica version for this node is out of date")
		} else {
			log.Printf("Warning: No existing replica for this node.")
		}
		return false, 0
	}

	//copy replica to new local file name
	fs, err := fileHandler.Copy(fileHandler.FilePath(filename), fileHandler.LocalFilePath(localfilename))
	if err != nil {
		log.Printf("Could not copy the replica to the local file")
		return false, 0
	}

	//Move the new copy to fetchedfiles
	newFetchedFile := FetchedFileMetadata{
		FileName:      filename,
		FileVersion:   fileVersion,
		LocalFileName: localfilename,
		FileSize:      int32(fs),
	}

	fetchedFiles = append(fetchedFiles, newFetchedFile)
	return true, newFetchedFile.FileSize
}

// Check version of fetched file existed on local storage
func lookupLocalFetchedFile(filename string, fileVersion int32, localfilename string) (bool, int32) {
	var fetchedFilesWithSameVersion []FetchedFileMetadata
	var identicalLocalFetchedFile FetchedFileMetadata

	for _, ff := range fetchedFiles {
		if ff.FileName == filename && ff.FileVersion == fileVersion {
			if ff.LocalFileName == localfilename {
				identicalLocalFetchedFile = ff
				break
			}
			fetchedFilesWithSameVersion = append(fetchedFilesWithSameVersion, ff)
		}
	}

	// return it if already existed
	if identicalLocalFetchedFile.FileName != "" {
		return true, identicalLocalFetchedFile.FileSize
	}

	if len(fetchedFilesWithSameVersion) == 0 {
		return false, 0
	}

	// If there is a fetched file with same version but with different name,
	// then copy that fetched file to a new file named localfilename
	firstFetchedFile := fetchedFilesWithSameVersion[0]
	fs, err := fileHandler.Copy(firstFetchedFile.LocalFileName, localfilename)
	if err != nil {
		log.Printf("Unable to copy file %s to file %s on local storage", firstFetchedFile.LocalFileName, localfilename)
		return false, 0
	}

	// Copy to fetchedfiles
	newFetchedFile := FetchedFileMetadata{
		FileName:      filename,
		FileVersion:   fileVersion,
		LocalFileName: localfilename,
		FileSize:      int32(fs),
	}

	fetchedFiles = append(fetchedFiles, newFetchedFile)
	return true, newFetchedFile.FileSize
}

func readFileFromReplica(filename string, localfilename string, fileVersion int32, replicaNode string) int32 {
	//gRPC call
	readRequest := &pb.ReadRequest{
		FileName:    filename,
		FileVersion: int32(fileVersion),
	}

	log.Printf("Send read request for file %s to replica '%s'.\n", filename, replicaNode)

	cc, err := grpc.NewClient(fmt.Sprintf("%s:%d", replicaNode, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMessage := fmt.Sprintf("Could not connect to replica node at %s. Error: %v\n", replicaNode, err.Error())
		log.Printf("Cannot remove file '%s'. Error: %s\n", filename, errorMessage)
		return 0
	}

	defer cc.Close()

	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewFileServiceClient(cc)

	resStream, err := c.Read(ctx, readRequest)

	if err != nil {
		log.Println("Error: It is unable acquire read file ", filename, ". Error: ", err)
		return 0
	}

	file := fileHandler.CreateOrOpenFile(fileHandler.LocalFilePath(localfilename))
	defer file.Close()

	// Stream data response from the log agent
	fileSize := 0
	log.Print("Reading...")
	counting := 0
	for {
		msg, err := resStream.Recv()
		if err == io.EOF {
			break
		}

		// Log error
		if err != nil {
			log.Printf("Error while reading stream: %v\n", err.Error())
			return 0
		}

		// Read a data chunk from the streaming response
		nb, err := file.Write(msg.GetFileValue())
		if err != nil {
			log.Printf("Error while writing to file: %v\n", err.Error())
		}

		fileSize = fileSize + nb
		if counting%10000 == 0 {
			fmt.Print(".")
		}
		counting++
	}
	log.Printf("Write '%d' byte from replica '%s' to file '%s' \n", fileSize, replicaNode, localfilename)

	newFetchedFile := FetchedFileMetadata{
		FileName:      filename,
		FileVersion:   fileVersion,
		LocalFileName: localfilename,
		FileSize:      int32(fileSize),
	}

	fetchedFiles = append(fetchedFiles, newFetchedFile)

	return int32(fileSize)
}

// Scan user interaction
func scan(in chan string) {
	var input string
	_, err := fmt.Scanln(&input)
	if err != nil {
		panic(err)
	}

	in <- input
}
