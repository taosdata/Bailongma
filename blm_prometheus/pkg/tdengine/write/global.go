/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

package write

import (
	"sync"
)

// main vars
var (
	BatchChannels   []chan string //multi table one chan
	BufPool         sync.Pool
	WorkersGroup    sync.WaitGroup
	IsSTableCreated sync.Map
	IsTableCreated  sync.Map
	TdUrl           string
	TagStr          string
	HttpWorkers     int
	SqlWorkers      int
)

// write vars
var (
	DaemonIP      string
	DaemonName    string
	BatchSize     int
	BufferSize    int
	DbName        string
	DbUser        string
	DbPassword    string
	RWPort        string
	ApiPort       string
	DebugPrt      int
	TagLen        int
	TagLimit      int = 1024
	TagNumLimit   int
	TablePervNode int
	DriverName    string
)
