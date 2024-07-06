package main

import (
	"bufio"
	"io"

	pkg "github.com/nathang15/go-fs/pkg"
)

var logger pkg.Logger
var scanner *bufio.Scanner
var mw io.Writer

func main() {
	logger.EnableLog()
}
