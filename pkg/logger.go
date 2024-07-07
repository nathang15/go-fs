package pkg

import (
	"fmt"
	"io"
	"log"
	"os"
)

type Logger struct{}

func (a *Logger) EnableLog() {
	fileName := fmt.Sprintf("../logs/log-%s.log", ConvertCurrentTimeToString())
	logOutput, _ := os.Create(fileName)
	mw := io.MultiWriter(os.Stdout, logOutput)
	log.SetOutput(mw)
}

func (a *Logger) DisableLog() {
	log.SetOutput(io.Discard)
}
