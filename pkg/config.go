package pkg

import (
	"fmt"
	"log"
	"net"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/tkanos/gonfig"
)

type Config struct {
	InstanceID                              string //instanceId of EC2 instance
	IPAddress                               string
	LogPath                                 string
	FilePath                                string
	LocalFilePath                           string
	HeartbeatLog                            bool
	Membershiplog                           bool
	HeartbeatIntervalTimeMs                 int
	HeartbeatIntervalTimeSecond             int
	NumberOfPredAndSucc                     int
	UDPPort                                 int
	NumShownHeartbeats                      int
	HeartbeatTimeoutInMs                    int
	HeartbeatAddr                           string
	TCPPort                                 int
	GatewayNodes                            []string
	HostAddress                             string
	Timeout                                 int
	FileChunkSize                           int
	WriteQuorum                             int
	ReadQuorum                              int
	NumberOfReplicas                        int
	PendingWriteTransactionTimeoutInSeconds int
	MaxDatagramSize                         int
}

func LoadConfig(configFile string) Config {
	config := Config{}
	err := gonfig.GetConf(configFile, &config)

	if err != nil {
		log.Fatalf("Failed to load configuration file: %v", err.Error())
	}

	instanceID, ipAddress := loadAWSConfiguration()

	config.InstanceID = instanceID
	config.IPAddress = ipAddress
	config.HostAddress = fmt.Sprintf("%s:%d", config.IPAddress, config.TCPPort)
	config.HeartbeatAddr = fmt.Sprintf("%s:%d", config.IPAddress, config.UDPPort)

	return config
}

// Loading EC2 nodes using instanceId and private IP address
func loadAWSConfiguration() (instanceID string, ipAddress string) {
	//Load session from shared config
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := ec2metadata.New(sess)

	metadata, err := svc.GetInstanceIdentityDocument()

	if err != nil {
		// use default configuration if unable to load ec2 metadata
		log.Println("Unable to load current metadata", err.Error())
		return "node0", getOutboundIP()
	}

	return metadata.InstanceID, metadata.PrivateIP
}

func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	log.Println("local ip address = ", localAddr.IP.String())
	return localAddr.IP.String()
}
