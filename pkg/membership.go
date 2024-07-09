package pkg

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "..pb"
	"google.golang.org/grpc"
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

type membershipServer struct{}

var config Config
var grpcServer *grpc.Server
var server Server
var membershipService MembershipService

func (m *MembershipService) GetNodeAddress() []string {
	var result []string
	for _, member := range membershipService.activeMembers.activeMembersByName {
		result = append(result, member)
	}
	return result
}

func (m *MembershipService) GetFollowerAddress() []string {
	var result []string
	for _, member := range membershipService.activeMembers.activeMembersByName {
		if member != masterNode {
			result = append(result, member)
		}
	}
	return result
}

func (m *MembershipService) GetActiveMembersNumber() int {
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

}

func (m *MembershipService) Stop() {

}

func (m *MembershipService) Report() {

}

func (*membershipServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {

}

func (*membershipServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {

}

// ----------------- Helper functions -----------------

func stopGrpcServer() {
	logging(fmt.Sprintf("Stopping membership service..."))
	grpcServer.Stop()
}

func sendLeaveReq(nodeAddress string, targetNodeAddres string, returnChan chan bool) {

}

func sendJoinReq(joinAddress string, targetNodeAddress string, returnChan chan bool) {

}
func printMembershipList() string {
	return fmt.Sprintf(
		"len=%d, members = %v ... %v\n",
		membershipService.activeMembers.len(),
		membershipService.activeMembers.getIds(),
		membershipService.activeMembers.getNames())
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

func (m *membersCounter) len() int {
	m.mux.Lock()
	defer m.mux.Unlock()
	return len(m.activeMembersByAddress)
}

func generateNodeName(nodeAddress string) string {
	return fmt.Sprintf("%s-%s", nodeAddress, ConvertCurrentTimeToString())
}

func logging(message string) {
	if config.HeartbeatLog {
		log.Print(message)
	}
}
