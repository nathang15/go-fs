package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"time"

	pkg "github.com/nathang15/go-fs/pkg"
)

var logger pkg.Logger
var scanner *bufio.Scanner
var mw io.Writer
var client pkg.Client

func main() {
	logger.EnableLog()
}

func processCmds() {
	init := flag.Bool("i", false, "Initialize")
	guest := flag.Bool("g", false, "Guest")

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

			time.Sleep(1 * time.Second)
		}

		exit := getNextCmd(*guest)
		if exit {
			break
		}
	}
}

// TODO: UNFINISHED
func startService(init bool, guest bool) {
	if !guest {

	} else {
		client.Init()
	}
}

// TODO: USER LOGIC
func getNextCmd(guest bool) bool {
	return true
}
