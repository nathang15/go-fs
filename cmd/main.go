package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	lib "github.com/nathang15/go-fs/pkg"
)

var config lib.Config
var membership lib.MembershipService
var client lib.Client
var server lib.Server
var logger lib.Logger
var scanner *bufio.Scanner
var mw io.Writer

// handle file commands
func main() {
	logger.EnableLog()

	config = lib.LoadConfig("../cmd/config.json")

	membership.SetConfig(config)
	// process user commands
	processUserCommands()
}

// process the first user command (init a new membership service or join an existing one). Loop to acquire the next commands
func processUserCommands() {
	init := flag.Bool("i", false, "init group")
	guest := flag.Bool("g", false, "a guest")

	flag.Parse()

	startService(*init, *guest)
	scanner = bufio.NewScanner(os.Stdin)

	for {
		if !*guest {
			scanner.Scan()
			nextOption := scanner.Text()

			if nextOption != "c" {
				break
			}
		}
		exit := acquireNextCommand(*guest)
		if exit {
			break
		}
	}
}

// start the file system
// -init: If true, init new membership service group. Otherwise, join an existing one by sending join request to master node
func startService(init bool, guest bool) {
	if !guest {
		membership.Start(init)
	} else {
		client.Init(config.GatewayNodes[0], config)
	}
}

func stopService() {
	membership.Stop()
}

func acquireNextCommand(guest bool) bool {

	if !guest {
		if membership.Len() == 0 {
			fmt.Println("File system is stopped. Do you want to start again?(yes/no) ")
		} else {
			fmt.Println("File system is started. What do you want to do next? (stop | report | file operations) ")
		}
	} else {
		fmt.Println("which operation do you want to perform?")
	}

	scanner.Scan()
	nextOption := scanner.Text()

	if !guest {
		if membership.Len() == 0 {
			if strings.EqualFold(nextOption, "yes") || strings.EqualFold(nextOption, "y") {
				startService(false, false)
				return false
			}
			fmt.Println("Exiting...")
			return true
		}
	}

	switch nextOption {
	case "exit":
		fmt.Printf("Exiting...\n")
		return true
	case "stop":
		stopService()
		return false
	case "report":
		if !guest {
			membership.Report()
		}
		client.Report()
		return false
	case "ls":
		client.PrintAllFiles(client.ListAllFiles())
		return false
	case "lshere":
		client.ListHere()
		return false
	default:
		nextOptionsParams := strings.Split(nextOption, " ")
		switch nextOptionsParams[0] {
		case "put":
			if len(nextOptionsParams) != 3 {
				break
			}
			localfilename := nextOptionsParams[1]
			fsfilename := nextOptionsParams[2]

			// call put operation handler
			client.Put(localfilename, fsfilename)
			return false
		case "get":
			if len(nextOptionsParams) != 3 {
				break
			}
			localfilename := nextOptionsParams[2]
			fsfilename := nextOptionsParams[1]

			// call get operation handler
			client.Fetch(fsfilename, localfilename)
			return false
		case "remove":
			if len(nextOptionsParams) != 2 {
				break
			}
			fsfilename := nextOptionsParams[1]

			client.RemoveFile(fsfilename)
			return false

		case "locate":
			if len(nextOptionsParams) != 2 {
				break
			}
			fsfilename := nextOptionsParams[1]

			client.PrintLocateFile(fsfilename)
			return false

		default:
			fmt.Printf("Unrecognized option %s, try again...\n", nextOption)
			return false
		}
	}

	return true
}
