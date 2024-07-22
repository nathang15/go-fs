package pkg

type Config struct {
	IPAddress                   string
	LogPath                     string
	FilePath                    string
	LocalFilePath               string
	HeartbeatLog                bool
	Membershiplog               bool
	HeartbeatIntervalTimeMs     int
	HeartbeatIntervalTimeSecond int
	NumberOfPredAndSucc         int
	UDPPort                     int
	NumShownHeartbeats          int
	HeartbeatTimeoutInMs        int
	HeartbeatAddr               string
}
