package pkg

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/nathang15/go-fs/pb"
	"google.golang.org/protobuf/proto"
)

type MonitorPacket struct {
	NodeAddress      string
	TimeLastReceived time.Time
	Count            int32
}

type counter struct {
	prevPackets []MonitorPacket
	mux         sync.Mutex
}

var prevPackets counter
var enableSimMode bool
var msgDroppedRate float64

func (c *counter) set(index int, packet MonitorPacket) {
	c.mux.Lock()
	c.prevPackets[index].Count = packet.Count
	c.prevPackets[index].NodeAddress = packet.NodeAddress
	c.prevPackets[index].TimeLastReceived = time.Now()
	c.mux.Unlock()
}

func (c *counter) getValues() []MonitorPacket {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.prevPackets
}

func (c *counter) get(index int) MonitorPacket {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.prevPackets[index]
}

func (c *counter) update(index int, heartbeatCount int32) {
	c.mux.Lock()
	c.prevPackets[index].Count = heartbeatCount
	c.prevPackets[index].TimeLastReceived = time.Now()
	c.mux.Unlock()
}

func startSendingHeartbeats() {
	heartbeatLog(fmt.Sprintln("Starting to send heartbeats"))
	for {
		if !membershipService.membershipStatus {
			heartbeatLog(fmt.Sprintln("Membership service is not active"))
			break
		}

		if membershipService.Len() <= 1 {
			time.Sleep(time.Duration(config.HeartbeatIntervalTimeMs) * time.Millisecond)
			continue
		}

		membershipService.restartSendingHb = false

		memberIndex := getMemberIndex(config.IPAddress)
		c := make(chan bool)
		activeMembersByAddrCount := membershipService.Len()
		numSuccessors := 0

		for i := 0; i < config.NumberOfPredAndSucc; i++ {
			if activeMembersByAddrCount < i+2 {
				break
			}
			nextMemberIndex := getMemberOffsetIndex(memberIndex, i+1)
			heartbeatLog(fmt.Sprintf("Sending heartbeat to node %d at '%s', every %d ms.\n", i+1, membershipService.activeMembers.getIds()[nextMemberIndex], config.HeartbeatIntervalTimeMs))
			go sendHeartbeat(membershipService.activeMembers.getIds()[nextMemberIndex], int64(i), c)
			numSuccessors++
		}

		for i := 0; i < numSuccessors; i++ {
			<-c
		}

		if membershipService.membershipStatus {
			heartbeatLog(fmt.Sprintln("Restarting because of modified membership list"))
		}
	}
}

func sendHeartbeat(targetNodeAddr string, nodeSeed int64, c chan bool) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", targetNodeAddr, config.UDPPort))
	checkError(err)

	conn, err := net.DialUDP("udp", nil, addr)

	defer conn.Close()

	count := 0
	countTo50 := 0
	droppedMsgList := []int{}

	for {
		if enableSimMode && countTo50 == 0 {
			droppedMsgList = generateRandomDroppedMsgList(nodeSeed)
		}

		msgDropped := shouldDropMsg(droppedMsgList, countTo50)
		if !enableSimMode || !msgDropped {
			heartbeatPkt := &pb.HeartbeatPacket{HbCount: int32(count), SentTime: time.Now().Unix()}
			data, err := proto.Marshal(heartbeatPkt)
			if err != nil {
				heartbeatLog(fmt.Sprintln("Error in marshalling: ", err))
			}

			buf := []byte(data)

			if heartbeatPkt.GetHbCount() < int32(config.NumShownHeartbeats) {
				heartbeatLog(fmt.Sprintf("Send heartbeat to node '%s' at '%s', count = %d", addr.String(), ConvertCurrentTimeToString(), heartbeatPkt.GetHbCount()))
			}

			if count == 0 {
				heartbeatLog(fmt.Sprintf("Heartbeat packet size = ", len(data)))
			}

			_, err = conn.Write(buf)
			if err != nil {
				heartbeatLog(fmt.Sprintln("Error in sending heartbeat: ", err))

				if membershipService.restartSendingHb || !membershipService.membershipStatus {
					heartbeatLog(fmt.Sprintf("Stop sending heartbeat to node '%s' at '%s' because of membership change"))
					c <- true
					break
				}
			}
		}

		if enableSimMode && msgDropped {
			heartbeatLog(fmt.Sprintf("Drop heartbeat packet '%d' to node '%s' (%d/50)\n", count, addr.String(), countTo50))
		}

		count++
		countTo50 = (countTo50 + 1) % 50

		time.Sleep(time.Duration(config.HeartbeatIntervalTimeMs) * time.Millisecond)

		if membershipService.restartSendingHb {
			c <- true
			break
		}
	}
}

func shouldDropMsg(droppedMsgList []int, msgNum int) bool {
	for _, i := range droppedMsgList {
		if i == msgNum {
			return true
		}
	}

	return false
}

func generateRandomDroppedMsgList(nodeSeed int64) []int {
	droppedMsgList := []int{}

	seed := nodeSeed + time.Now().UnixNano()
	s1 := rand.NewSource(seed)
	r1 := rand.New(s1)

	switch msgDroppedRate {
	case 0.1, 0.3:
		for j := 0; j < 5; j++ {
			for i := 0; i < int(msgDroppedRate*10); i++ {
				n := r1.Intn(10)
				m := n + j*10
				droppedMsgList = append(droppedMsgList, m)
			}
		}
	default:
		for i := 0; i < 2; i++ {
			n := r1.Intn(50)
			droppedMsgList = append(droppedMsgList, n)
		}
	}
	return droppedMsgList
}

func startMonitorHbMsg() {
	heartbeatLog(fmt.Sprintln("Start monitoring heartbeats"))
	membershipService.restartMonitoringHb = false

	initPreds()

	c := make(chan bool)

	go listenHeartbeats(c)

	for {
		if !membershipService.membershipStatus {
			heartbeatLog(fmt.Sprintln("Stop monitoring heartbeats due to inactive membership"))
			<-c
			heartbeatLog(fmt.Sprintln("Stopped monitoring heartbeats"))
			break
		}

		if membershipService.restartMonitoringHb {
			if membershipService.Len() >= 2 {
				heartbeatLog(fmt.Sprintln("Restart monitoring heartbeats due to changes in membership list"))
			}
			<-c
			membershipService.restartMonitoringHb = false

			c = make(chan bool)
			go listenHeartbeats(c)
		}

		if membershipService.Len() <= 1 {
			time.Sleep(time.Duration(config.HeartbeatIntervalTimeMs) * time.Millisecond)
			continue
		}

		countNum := 0
		for idx, pred := range prevPackets.getValues() {
			if pred.NodeAddress == "" {
				continue
			}

			if failed(pred) {
				countNum++
				notifyFail(pred.NodeAddress)
				prevPackets.set(idx, MonitorPacket{})
			}
		}
		time.Sleep(time.Duration(config.HeartbeatIntervalTimeMs) * time.Millisecond)
	}
}

func initPreds() {
	size := config.NumberOfPredAndSucc
	initPackets := []MonitorPacket{}
	for idx := 0; idx < size; idx++ {
		packet := MonitorPacket{
			NodeAddress:      "",
			TimeLastReceived: time.Now(),
			Count:            0,
		}
		initPackets = append(initPackets, packet)
	}
	prevPackets = counter{prevPackets: initPackets}
}

func listenHeartbeats(c chan bool) {
	if membershipService.Len() <= 1 {
		c <- true
		return
	}

	addr, err := net.ResolveUDPAddr("udp", config.HeartbeatAddr)
	checkError(err)
	l, err := net.ListenUDP("udp", addr)

	memberIdx := getMemberIndex(config.IPAddress)
	activeMembersByAddrCount := membershipService.Len()
	initPreds()

	for i := 0; i < config.NumberOfPredAndSucc; i++ {
		if activeMembersByAddrCount < i+2 {
			break
		}
		prevMemberIdx := getMemberOffsetIndex(memberIdx, -1*(i+1))

		prevPackets.set(i, MonitorPacket{
			NodeAddress:      membershipService.activeMembers.getIds()[prevMemberIdx],
			TimeLastReceived: time.Now(),
			Count:            0,
		})
		heartbeatLog(fmt.Sprint("Start listening to predecessor '%d' from '%s' at '%s'. \n", i+1,
			prevPackets.get(i).NodeAddress,
			prevPackets.get(i).TimeLastReceived.Format(dateTimeFormat)))
	}

	for {
		if !membershipService.membershipStatus {
			heartbeatLog(fmt.Sprintln("Stop monitoring heartbeats since membership status is inactive now. 1111"))
			l.Close()
			c <- true
			return
		}

		if membershipService.restartMonitoringHb {
			heartbeatLog(fmt.Sprintf("Restart monitoring heartbeats since active members list has changed.2222"))
			l.Close()
			c <- true
			return
		}

		buf := make([]byte, 1024)

		err := l.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err != nil {
			heartbeatLog(fmt.Sprintln("Error when set read deadline", err))
		}

		byteSize, remoteAddr, err := l.ReadFromUDP(buf)

		if err != nil {
			heartbeatLog(fmt.Sprintln("Uncaught error while listening to heartbeat. Connection is closed. ", err))
			l.Close()
			c <- true
			return
		}
		hbPacket := &pb.HeartbeatPacket{}
		err = proto.Unmarshal(buf[0:byteSize], hbPacket)
		if hbPacket.GetHbCount() < int32(config.NumShownHeartbeats) {
			heartbeatLog(fmt.Sprintf("Receive heartbeat from '%s', at '%s', count =  %d.\n", remoteAddr.String(), time.Unix((*hbPacket).GetSentTime(), 0).Format(dateTimeFormat), (*hbPacket).GetHbCount()))
		}

		if err != nil {
			heartbeatLog(fmt.Sprintln("Monitoring heartbeat process has failed due to unknown error:", err))
			c <- true
			l.Close()
		} else {
			for idx, pred := range prevPackets.getValues() {
				if pred.NodeAddress == remoteAddr.IP.String() {
					prevPackets.update(idx, hbPacket.GetHbCount())
				}
			}
		}
	}
}

func failed(monitorPacket MonitorPacket) bool {
	if monitorPacket.NodeAddress == "" {
		return false
	}

	timeDiff := time.Now().Sub(monitorPacket.TimeLastReceived)
	diffInMs := int64(timeDiff / time.Millisecond)
	if diffInMs > int64(config.HeartbeatTimeoutInMs) {
		heartbeatLog(fmt.Sprintf("DETECT FAILURE of node '%s' at '%s', last time received heartbeat is '%s', count = %d, diff = %d", monitorPacket.NodeAddress, time.Now().Format(dateTimeFormat), monitorPacket.TimeLastReceived.Format(time.RFC3339), monitorPacket.Count, diffInMs))
		return true
	}

	return false
}

func notifyFail(failedNodeAddr string) {
	heartbeatLog(fmt.Sprintf("Start notifying failure of node '%s' at '%s'", failedNodeAddr, time.Now().Format(dateTimeFormat)))
	failNodeIndex := getMemberIndex(failedNodeAddr)
	membershipService.activeMembers.removeMemberAtIndex(failNodeIndex)

	if failedNodeAddr == masterNode && membershipService.Len() > 1 {
		server.StartElection()
	}

	c := make(chan bool)
	count := 0

	for _, member := range membershipService.activeMembers.getIds() {
		if config.IPAddress == member {
			if config.IPAddress == masterNode {
				server.RemoveNodeLeave(failedNodeAddr)
			}
			continue
		}
		count++
		go sendLeaveReq(failedNodeAddr, member, c)
	}

	for i := 0; i < count; i++ {
		<-c
	}

}

func checkError(err error) {
	if err != nil {
		heartbeatLog(fmt.Sprintln("Error: ", err))
	}
}

func heartbeatLog(message string) {
	if config.HeartbeatLog {
		log.Printf(message)
	}
}
