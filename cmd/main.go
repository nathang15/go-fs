package main

import (
	"bufio"
	"io"

	pkg "../pkg"
)

var logger pkg.Logger
var scanner *bufio.Scanner
var mw io.Writer

func main() {
	logger.EnableLog()
}
