package log

import (
	"testing"
)

func TestInit(t *testing.T) {
	Init("./test.log")
	DebugLogger.Println("debug")
	InfoLogger.Println("info")
	ErrorLogger.Println("error")
	WarningLogger.Println("warning")
}
