/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package log

import (
	"fmt"
	"io"
	"log"
	"os"
)

const (
	flag       = log.Ldate | log.Ltime | log.Lshortfile
	preDebug   = "[DEBUG] "
	preInfo    = "[INFO] "
	preWarning = "[WARNING] "
	preError   = "[ERROR] "
)

var (
	logFile       io.Writer
	debugLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger
)

func Init(fileName string) {
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	debugLogger = log.New(logFile, preDebug, flag)
	infoLogger = log.New(logFile, preInfo, flag)
	warningLogger = log.New(logFile, preWarning, flag)
	errorLogger = log.New(logFile, preError, flag)
}

func Debugf(format string, v ...interface{}) {
	debugLogger.Printf(format, v...)
}

func Debugln(v ...interface{}) {
	debugLogger.Println(v...)
}

func Infof(format string, v ...interface{}) {
	infoLogger.Printf(format, v...)
}

func Infoln(v ...interface{}) {
	infoLogger.Println(v...)
}

func Warningf(format string, v ...interface{}) {
	warningLogger.Printf(format, v...)
}

func Errorf(format string, v ...interface{}) {
	errorLogger.Printf(format, v...)
}

func Warningln(v ...interface{}) {
	warningLogger.Println(v...)
}

func Errorln(v ...interface{}) {
	errorLogger.Println(v...)
}
