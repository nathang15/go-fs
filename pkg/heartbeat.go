package pkg

import (
	"sync"
	"time"
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

func (c *counter) updateHeartbeatVal(index int, packet MonitorPacket) {
	c.mux.Lock()
	c.prevPackets[index].Count = packet.Count
	c.prevPackets[index].NodeAddress = packet.NodeAddress
	c.prevPackets[index].TimeLastReceived = time.Now()
	c.mux.Unlock()
}

func (c *counter) getHeartbeatVal() []MonitorPacket {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.prevPackets
}

func sendHeartbeatToPrev() {
}

func sendHeartbeat(targetNode string, nodeSeed int64, c chan bool) {

}
