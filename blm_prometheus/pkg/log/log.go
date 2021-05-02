package log

import (
	"fmt"
	"log"
	"os"
)

var (
	logger *log.Logger
)

//func GetInstance(fileName string) *log.Logger {
//	once.Do(func() {
//		logger = Init(fileName)
//	})
//	return logger
//}

func Init(fileName string) *log.Logger {
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	logger = log.New(logFile, "", log.LstdFlags)
	logger.SetPrefix("BLM_PRM")
	logger.SetFlags(log.LstdFlags | log.Lshortfile)
	return logger
}
