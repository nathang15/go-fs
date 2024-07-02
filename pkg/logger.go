package pkg

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

const dateTimeFormat = "2024-05-01 15.04.05"

const longDateTimeFormat = "2024-05-01 15:04:05.000"

type Logger struct{}

func ConvertCurrentTimeToString() string {
	return time.Now().Format(dateTimeFormat)
}

func (a *Logger) EnableLog() {
	fileName := fmt.Sprintf("../logs/log-%s.log", ConvertCurrentTimeToString())
	logOutput, _ := os.Create(fileName)
	mw := io.MultiWriter(os.Stdout, logOutput)
	log.SetOutput(mw)
}

func (a *Logger) DisableLog() {
	log.SetOutput(io.Discard)
}
