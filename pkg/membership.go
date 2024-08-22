package pkg

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/nathang15/go-fs/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type membersCounter struct {
	activeMembersByAddress []string
	activeMembersByName    []string
	mux                    sync.Mutex
}

type MembershipService struct {
	activeMembers    membersCounter
	membershipStatus bool

	restartSendingHb    bool
	restartMonitoringHb bool
}

type membershipServer struct {
	pb.UnimplementedMemberShipServiceServer
}

var config Config
var grpcServer *grpc.Server
var server Server
var membershipService MembershipService

func (m *MembershipService) GetNodeAddresses() []string {
	var result []string
	for _, member := range membershipService.activeMembers.activeMembersByName {
		result = append(result, member)
	}
	return result
}

func (m *MembershipService) GetFollowerAddresses() []string {
	var result []string
	for _, member := range membershipService.activeMembers.activeMembersByAddress {
		if member == masterNode {
			continue
		}
		result = append(result, member)
	}
	return result
}

func (m *MembershipService) Len() int {
	return membershipService.activeMembers.len()
}

func (m *MembershipService) SetConfig(config Config) {
	config = config
}

func (m *MembershipService) Init(byAddress []string, byName []string) {
	if byAddress != nil {
		membershipService.activeMembers.init([]string{config.IPAddress}, []string{generateNodeName(config.IPAddress)})
		logging(fmt.Sprintf("Init membership list: %s.\n", printMembershipList()))
	} else {
		membershipService.activeMembers.init(byAddress, byName)
	}

	membershipService.membershipStatus = true
	membershipService.restartSendingHb = false
	membershipService.restartMonitoringHb = false

	masterNode = config.IPAddress
}

func (m *MembershipService) Start(init bool) {
	if init {
		membershipService.Init(nil, nil)
		server.Start(config.IPAddress, 0, []FileMetadata{}, []WriteTransaction{})
		fileHandler.ClearDB()
	} else {
		for _, node := range config.GatewayNodes {
			logging(fmt.Sprintln("Start sending join request to gateway node: ", node))
			isSuccessful, membersList, master, eTerm, initFiles, initPendingWriteTransactions := sendJoinReq(config.IPAddress, "", node, true)
			if isSuccessful {
				activeMembersByAddress := []string{}
				activeMembersByName := membersList
				for _, memberByName := range activeMembersByName {
					memberArr := strings.Split(memberByName, "-")
					activeMembersByAddress = append(activeMembersByAddress, memberArr[0])
				}
				membershipService.Init(activeMembersByAddress, activeMembersByName)
				server.Start(master, eTerm, initFiles, initPendingWriteTransactions)
				fileHandler.ClearDB()
				logging(fmt.Sprintln("Finish join request. New members list: ", printMembershipList()))
				break
			}
		}
	}
	startServices()
}

func (m *MembershipService) Stop() {
	membershipService.membershipStatus = false
	membershipService.restartSendingHb = true
	membershipService.restartMonitoringHb = true

	if isMasterNode() {
		logging(fmt.Sprintf("Start removing process for master node: %s\n", config.IPAddress))
	} else {
		logging(fmt.Sprintf("Start leaving process for node: %s\n", config.IPAddress))
	}

	memberIndex := getMemberIndex(config.IPAddress)
	membershipService.activeMembers.removeMemberAtIndex(memberIndex)

	if isMasterNode() {
		server.Offboarding(config.IPAddress)
	}

	c := make(chan bool)
	count := 0
	for _, member := range membershipService.activeMembers.getIds() {
		count++
		go sendLeaveReq(config.IPAddress, member, c)
	}

	for i := 0; i < count; i++ {
		<-c
	}

	membershipService.Init([]string{}, []string{})
	stopMembershipService()
	server.Stop()

	logging(fmt.Sprintln("Finish removal process"))
}

func (m *MembershipService) Report() {
	log.Println("----------------- Membership Service Report -----------------")
	logging(fmt.Sprintln("Members: ", printMembershipList()))
}

func (*membershipServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	nodeAddr := req.GetNodeAddress()
	nodeName := req.GetNodeName()
	if nodeName == "" {
		nodeName = generateNodeName(nodeAddr)
	}

	logging(fmt.Sprintf("Receive join request from '%s' with option spreading new join is %t.\n", nodeAddr, req.GetNeedSpreading()))

	if getMemberIndex(req.GetNodeAddress()) == -1 {
		if req.GetNeedSpreading() {
			for _, targetNodeAddr := range membershipService.activeMembers.getIds() {
				if targetNodeAddr != config.IPAddress {
					logging(fmt.Sprintf("Start updating about new join request to node %s: %d.\n", targetNodeAddr, config.TCPPort))
					go sendJoinReq(nodeAddr, nodeName, targetNodeAddr, false)
				}
			}
		}
		membershipService.activeMembers.addMember(req.GetNodeAddress(), nodeName)
		server.UpdateReplicaNodes(nodeAddr)

	} else {
		err := fmt.Sprintf("Node %s already exists in the membership list.\n", nodeAddr)
		logging(fmt.Sprintln(err))
		return nil, status.Errorf(codes.Internal, err)
	}

	response := &pb.JoinResponse{}

	if !req.NeedSpreading {
		return response, nil
	}

	response.Members = membershipService.activeMembers.getNames()

	var bpFiles []*pb.JoinResponse_FileMetadata
	var bpPendingWriteTransactions []*pb.JoinResponse_WriteTransaction

	for _, file := range files {
		bpFile := &pb.JoinResponse_FileMetadata{
			FileName:        file.FileName,
			FileSize:        file.FileSize,
			FileVersion:     file.FileVersion,
			PreferenceNodes: file.PreferenceNodes,
		}
		bpFiles = append(bpFiles, bpFile)
	}

	for _, pendingWriteTransaction := range pendingWriteTransactions {
		bpPendingWriteTransaction := &pb.JoinResponse_WriteTransaction{
			FileName:        pendingWriteTransaction.FileName,
			FileVersion:     pendingWriteTransaction.FileVersion,
			RequestID:       pendingWriteTransaction.RequestID,
			PreferenceNodes: pendingWriteTransaction.PreferenceNodes,
			RegisteredTime:  pendingWriteTransaction.RegisteredTime.UnixNano() / int64(time.Millisecond),
		}
		bpPendingWriteTransactions = append(bpPendingWriteTransactions, bpPendingWriteTransaction)
	}

	response.Files = bpFiles
	response.PendingWriteTransactions = bpPendingWriteTransactions
	response.MasterNode = masterNode
	response.ElectionTerm = electionTerm

	return response, nil
}

func (*membershipServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	logging(fmt.Sprintln("Received leave request for node", req.GetLeavingNodeAddress(), " from node: ", req.GetRequesterNodeAddress()))
	memberIndex := getMemberIndex(req.GetLeavingNodeAddress())
	if memberIndex == -1 {
		err := fmt.Sprintf("Warning: This node '%s' is already removed from active members list.\n", req.GetLeavingNodeAddress())
		logging(fmt.Sprintln(err))
		response := &pb.LeaveResponse{
			LeaveStatus:  false,
			ErrorMessage: err,
		}

		return response, status.Errorf(codes.Internal, err)
	}
	requestMemberIndex := getMemberIndex(req.GetRequesterNodeAddress())
	if requestMemberIndex == -1 {
		err := fmt.Sprintf("Warning : This node '%s' is not allowed to send leave request.\n", req.GetLeavingNodeAddress())
		logging(fmt.Sprintln(err))
		response := &pb.LeaveResponse{
			LeaveStatus:  false,
			ErrorMessage: err,
		}
		return response, status.Errorf(codes.Internal, err)
	}

	membershipService.activeMembers.removeMemberAtIndex(memberIndex)
	if isMasterNode() {
		server.Offboarding(req.GetLeavingNodeAddress())
	}

	for idx, pred := range prevPackets.getValues() {
		if pred.NodeAddress == req.GetLeavingNodeAddress() {
			prevPackets.set(idx, MonitorPacket{})
		}
	}

	response := &pb.LeaveResponse{
		LeaveStatus: true,
	}

	return response, nil
}

// ----------------- Helper functions -----------------

func stopMembershipService() {
	logging(fmt.Sprintf("Stopping membership service..."))
	grpcServer.Stop()
}

func sendLeaveReq(nodeAddress string, targetNodeAddress string, returnChan chan bool) {
	logging(fmt.Sprintf("Start sending leave requet to node '%s'.\n", targetNodeAddress))

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddress, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errMsg := fmt.Sprintf("Failed to dial to node '%s': %v.\n", targetNodeAddress, err.Error())
		logging(fmt.Sprintf("Cannot join to membership service because ", errMsg))
	}
	defer cc.Close()

	req := &pb.LeaveRequest{
		LeavingNodeAddress:   nodeAddress,
		RequesterNodeAddress: config.IPAddress,
	}
	reqSize := proto.Size(req)
	logging(fmt.Sprintf("Leave request size: %d\n", reqSize))

	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewMemberShipServiceClient(cc)

	leaveResponse, err := c.Leave(ctx, req)

	responseSize := proto.Size(leaveResponse)
	logging(fmt.Sprintln("Leave response size: ", responseSize))

	if !leaveResponse.GetLeaveStatus() {
		logging(fmt.Sprintln("Finish sending leave request to node ", targetNodeAddress, " with error: ", leaveResponse.GetErrorMessage()))
	}
	logging(fmt.Sprintf("Finish sending leave request to node '%s'.\n", targetNodeAddress))
	returnChan <- true
}

func sendJoinReq(joinAddr string, joinName string, targetNodeAddr string, spreading bool) (bool, []string, string, int32, []FileMetadata, []WriteTransaction) {
	request := &pb.JoinRequest{
		NodeAddress:   joinAddr,
		NodeName:      joinName,
		NeedSpreading: spreading,
	}

	requestSize := proto.Size(request)
	logging(fmt.Sprintf("Join Request for node '%s' to node '%s', size: %d, spreading: %t.\n", joinAddr, targetNodeAddr, requestSize, spreading))

	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", targetNodeAddr, config.TCPPort), grpc.WithInsecure())
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to dial to node '%s': %v.\n", targetNodeAddr, err.Error())
		logging(fmt.Sprintf("Cannot join to membership service because ", errorMsg))
		return false, []string{}, "", 0, nil, nil
	}

	defer cc.Close()

	duration := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	c := pb.NewMemberShipServiceClient(cc)
	joinResponse, err := c.Join(ctx, request)
	responseSize := proto.Size(joinResponse)
	logging(fmt.Sprintln("Join Response: size = ", responseSize, ", response = ", joinResponse))

	if err != nil {
		logging(fmt.Sprintln("Warning! Unable to process join for node ", joinAddr, "Error: ", err))
		return false, []string{}, "", 0, nil, nil
	}

	var initFiles []FileMetadata
	var initPendingWriteTransactions []WriteTransaction

	bpFiles := joinResponse.GetFiles()
	bpPendingWriteTransactions := joinResponse.GetPendingWriteTransactions()

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
	return true, joinResponse.GetMembers(), joinResponse.GetMasterNode(), joinResponse.GetElectionTerm(), initFiles, initPendingWriteTransactions
}

func printMembershipList() string {
	return fmt.Sprintf(
		"len=%d, members = %v ... %v\n",
		membershipService.activeMembers.len(),
		membershipService.activeMembers.getIds(),
		membershipService.activeMembers.getNames())
}

func startServices() {
	membershipService.membershipStatus = true
	membershipService.restartSendingHb = false
	membershipService.restartMonitoringHb = false

	go startMembershipService()
	go startSendingHeartbeats()
	go startMonitorHbMsg()
}

func startMembershipService() {
	l, err := net.Listen("tcp", config.HostAddress)
	defer l.Close()

	if err != nil {
		log.Fatalf("Failed to listen: %v", err.Error())
	}
	logging(fmt.Sprintf("Start membership service '%s' listening at '%s'.\n", config.InstanceID, config.HostAddress))
	logging(fmt.Sprintf("Start file service '%s' listening at '%s'.\n", config.InstanceID, config.HostAddress))

	grpcServer = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
	)

	pb.RegisterMemberShipServiceServer(grpcServer, &membershipServer{})
	pb.RegisterFileServiceServer(grpcServer, &grpcFileServer{})

	if err := grpcServer.Serve(l); err != nil {
		log.Fatalf("Failed to serve: %v", err.Error())
	}
}

func (m *membersCounter) init(byAddress []string, byName []string) {
	m.mux.Lock()
	m.activeMembersByAddress = byAddress
	m.activeMembersByName = byName
	m.mux.Unlock()
}

func (m *membersCounter) getIds() []string {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.activeMembersByAddress
}

func (m *membersCounter) getNames() []string {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.activeMembersByName
}

func getMemberIndex(memberAddress string) int {
	memberIndex := -1
	for i := 0; i < membershipService.Len(); i++ {
		if membershipService.activeMembers.getIds()[i] == memberAddress {
			memberIndex = i
			break
		}
	}
	return memberIndex
}

func (m *membersCounter) len() int {
	m.mux.Lock()
	defer m.mux.Unlock()
	return len(m.activeMembersByAddress)
}

func getMemberOffsetIndex(index int, offset int) int {
	return (index + offset + membershipService.Len()) % membershipService.Len()
}

func generateNodeName(nodeAddress string) string {
	return fmt.Sprintf("%s-%s", nodeAddress, ConvertCurrentTimeToString())
}

func (c *membersCounter) removeMemberAtIndex(index int) {
	c.mux.Lock()
	removeNodeName := c.activeMembersByName[index]

	c.activeMembersByAddress = append(c.activeMembersByName[:index], c.activeMembersByName[index+1:]...)

	membershipService.restartSendingHb = true
	membershipService.restartMonitoringHb = true
	logging(fmt.Sprintf("Remove node '%s' at '%s' (prev index = %d). Active members: len = %d, [%s ..... %s]\n", removeNodeName, ConvertCurrentTimeToString(), index, len(c.activeMembersByAddress), c.activeMembersByAddress, c.activeMembersByName))
	c.mux.Unlock()
}

func (c *membersCounter) addMember(nodeAddress string, nodeName string) {
	c.mux.Lock()
	c.activeMembersByAddress = append(c.activeMembersByAddress, nodeAddress)

	c.activeMembersByName = append(c.activeMembersByName, nodeName)

	if len(c.activeMembersByAddress) > 1 {
		membershipService.restartSendingHb = true
		membershipService.restartMonitoringHb = true

		logging(fmt.Sprintf("Add new join node '%s' at '%s'. Active members: len = %d, [%s ..... %s]\n", nodeName, ConvertCurrentTimeToString(), len(c.activeMembersByAddress), c.activeMembersByAddress, c.activeMembersByName))
	}

	c.mux.Unlock()
}

func logging(message string) {
	if config.HeartbeatLog {
		log.Print(message)
	}
}
