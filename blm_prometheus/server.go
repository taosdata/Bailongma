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

package main

import (
	"blm_prometheus/pkg/tdengine"
	"blm_prometheus/pkg/tdengine/write"
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	logger "blm_prometheus/pkg/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	_ "github.com/taosdata/driver-go/taosSql"
)

type fieldDescription struct {
	fileName   string
	fileType   string
	fileLength int
	fileNote   string
}

type tableStruct struct {
	status string
	head   []string
	data   []fieldDescription
	rows   int64
}

type reader interface {
	Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error)
	Name() string
}

type writer interface {
	Write(req *prompb.WriteRequest) error
	Name() string
	Check(r *http.Request) (string, error)
}

// Global vars
var (
	bufPool         sync.Pool
	nodeChannels    []chan prompb.WriteRequest //multi node one chan
	inputDone       chan struct{}
	reportTags      [][2]string
	reportHostname  string
	taosDriverName  string = "taosSql"
	IsSTableCreated sync.Map
	IsTableCreated  sync.Map
	tagStr          string
	tdUrl           string
	logNameDefault  string = "/var/log/taos/blm_prometheus.log"
)

var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// Parse args:
func init() {
	flag.StringVar(&write.DaemonIP, "tdengine-ip", "127.0.0.1", "TDengine host IP.")
	flag.StringVar(&write.DaemonName, "tdengine-name", "", "TDengine host Name. in K8S, could be used to lookup TDengine's IP")
	flag.StringVar(&write.ApiPort, "tdengine-api-port", "6041", "TDengine restful API port")
	flag.IntVar(&write.BatchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&write.HttpWorkers, "http-workers", 1, "Number of parallel http requests handler .")
	flag.IntVar(&write.SqlWorkers, "sql-workers", 1, "Number of parallel sql handler.")
	flag.StringVar(&write.DbName, "dbname", "prometheus", "Database name where to store metrics")
	flag.StringVar(&write.DbUser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&write.DbPassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&write.RWPort, "port", "10203", "remote write and read port")
	flag.IntVar(&write.DebugPrt, "debugprt", 0, "if 0 not print, if 1 print the sql")
	flag.IntVar(&write.TagLen, "tag-length", 128, "the max length of tag string,default is 30")
	flag.IntVar(&write.BufferSize, "buffersize", 100, "the buffer size of metrics received")
	flag.IntVar(&write.TagNumLimit, "tag-num", 128, "the number of tags in a super table, default is 8")
	//flag.IntVar(&tablepervnode, "table-num", 10000, "the number of tables per TDengine Vnode can create, default 10000")

	flag.Parse()
	write.DriverName = taosDriverName

	if write.DaemonName != "" {
		// look DNS A records
		s, _ := net.LookupIP(write.DaemonName)
		write.DaemonIP = fmt.Sprintf("%s", s[0])
		write.DaemonIP = write.DaemonIP + ":0"
		fmt.Println("daemonIP:" + write.DaemonIP)

		write.TdUrl = write.DaemonName
	} else {
		write.TdUrl = write.DaemonIP
		write.DaemonIP = write.DaemonIP + ":0"
	}

	write.TagStr = fmt.Sprintf(" binary(%d)", write.TagLen)
	// init logger
	logger.Init(logNameDefault)

	logger.InfoLogger.Print("host: ip")
	logger.InfoLogger.Print(write.DaemonIP)
	logger.InfoLogger.Print("  port: ")
	logger.InfoLogger.Print(write.RWPort)
	logger.InfoLogger.Print("  database: ")
	logger.InfoLogger.Println(write.DbName)

}

func main() {

	for i := 0; i < write.HttpWorkers; i++ {
		nodeChannels = append(nodeChannels, make(chan prompb.WriteRequest, write.BufferSize))
	}

	createDatabase(write.DbName)

	reader, writer := buildClients()

	for i := 0; i < write.HttpWorkers; i++ {
		write.WorkersGroup.Add(1)
		go NodeProcess(i, writer)
	}

	for i := 0; i < write.SqlWorkers; i++ {
		write.BatchChannels = append(write.BatchChannels, make(chan string, write.BatchSize))
	}

	for i := 0; i < write.SqlWorkers; i++ {
		write.WorkersGroup.Add(1)
		go processBatches(i)
	}

	http.Handle("/receive", writeHandle())
	http.Handle("/check", checkHandle(writer))
	http.Handle("/health", healthHandle())
	if write.DebugPrt == 5 {
		TestSerialization()
	}
	// read
	http.Handle("/pull", readHandle(reader))
	log.Fatal(http.ListenAndServe(":"+write.RWPort, nil))

}

func buildClients() (reader, writer) {
	tdClient := tdengine.NewClient()
	return tdClient, tdClient
}

func writeHandle() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		addr := strings.Split(r.RemoteAddr, ":")
		idx := TAOSHashID([]byte(addr[0]))

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		nodeChannels[idx%write.HttpWorkers] <- req
	})
}
func checkHandle(writer writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := writer.Check(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s := fmt.Sprintf("query result:\n %s\n", res)

		w.Write([]byte(s))
		w.WriteHeader(http.StatusOK)
	})
}
func readHandle(reader reader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			logger.ErrorLogger.Printf("Read error: %s\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			logger.ErrorLogger.Printf("Decode error: %s\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			logger.ErrorLogger.Printf("Unmarshal error: %s\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		logger.InfoLogger.Printf("req info== %v\n", req)
		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			logger.ErrorLogger.Printf("Error executing query req: %s,storage:%s,err:%s\n", req, reader.Name(), err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			logger.ErrorLogger.Printf("proto marshal err :%s\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			logger.ErrorLogger.Printf("Write response err :%s\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}
func healthHandle() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
}

func TAOSHashID(ba []byte) int {
	var sum int = 0
	for i := 0; i < len(ba); i++ {
		sum += int(ba[i] - '0')
	}
	return sum
}

func NodeProcess(workerId int, writer writer) error {
	for req := range nodeChannels[workerId] {
		ProcessReq(req, writer)
	}
	return nil
}

func ProcessReq(req prompb.WriteRequest, writer writer) error {
	return writer.Write(&req)
}

func createDatabase(dbName string) {
	db, err := sql.Open(write.DriverName, write.DbUser+":"+write.DbPassword+"@/tcp("+write.DaemonIP+")/")
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
	sqlcmd := fmt.Sprintf("create database if not exists %s ", dbName)
	_, err = db.Exec(sqlcmd)
	sqlcmd = fmt.Sprintf("use %s", dbName)
	_, err = db.Exec(sqlcmd)
	checkErr(err)
	return
}

func checkErr(err error) {
	if err != nil {
		logger.InfoLogger.Println(err)
	}
}

func processBatches(workerId int) {
	var i int
	db, err := sql.Open(write.DriverName, write.DbUser+":"+write.DbPassword+"@/tcp("+write.DaemonIP+")/"+write.DbName)
	if err != nil {
		logger.ErrorLogger.Printf("processBatches Open database error: %s\n", err)
		var count int = 5
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = sql.Open(taosDriverName, write.DbUser+":"+write.DbPassword+"@/tcp("+write.DaemonIP+")/"+write.DbName)
				count--
			} else {
				if err != nil {
					logger.ErrorLogger.Printf("processBatches Error: %s open database\n", err)
					return
				}
				break
			}
		}
	}
	defer db.Close()
	sqlcmd := make([]string, write.BatchSize+1)
	i = 0
	sqlcmd[i] = "Insert into"
	i++

	for onePoint := range write.BatchChannels[workerId] {
		sqlcmd[i] = onePoint
		i++
		if i > write.BatchSize {
			i = 1
			_, err := db.Exec(strings.Join(sqlcmd, ""))
			if err != nil {
				logger.ErrorLogger.Printf("processBatches error %s\n", err)
				var count int = 2
				for {
					if err != nil && count > 0 {
						<-time.After(time.Second * 1)
						_, err = db.Exec(strings.Join(sqlcmd, ""))
						count--
					} else {
						if err != nil {
							logger.ErrorLogger.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
						}
						break
					}

				}
			}
		}
	}
	if i > 1 {
		i = 1
		_, err := db.Exec(strings.Join(sqlcmd, ""))
		if err != nil {
			var count int = 2
			for {
				if err != nil && count > 0 {
					<-time.After(time.Second * 1)
					_, err = db.Exec(strings.Join(sqlcmd, ""))
					count--
				} else {
					if err != nil {
						logger.ErrorLogger.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
					}
					break
				}
			}
		}
	}

	write.WorkersGroup.Done()
}

func TestSerialization() {
	var req prompb.WriteRequest
	var ts []*prompb.TimeSeries
	var tse prompb.TimeSeries
	var sample *prompb.Sample
	var label prompb.Label
	var lbs []*prompb.Label
	promPath, err := os.Getwd()
	if err != nil {
		fmt.Printf("can't get current dir :%s \n", err)
		os.Exit(1)
	}
	promPath = filepath.Join(promPath, "testData/blm_prometheus.log")
	testFile, err := os.OpenFile(promPath, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open file error!", err)
		return
	}
	defer testFile.Close()
	fmt.Println(promPath)
	buf := bufio.NewReader(testFile)
	i := 0
	lastTime := "20:40:20"
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("File read ok! line:", i)
				break
			} else {
				fmt.Println("Read file error!", err)
				return
			}
		}
		sa := strings.Split(line, " ")

		if strings.Contains(line, "server.go:201:") {
			if sa[3] != lastTime {
				nodeChannels[0] <- req
				lastTime = sa[3]
				req.Timeseries = req.Timeseries[:0]
				ts = ts[:0]
			}
			tse.Samples = make([]prompb.Sample, 0)
			T, _ := strconv.ParseInt(sa[7][:(len(sa[7])-1)], 10, 64)
			V, _ := strconv.ParseFloat(sa[9][:(len(sa[9])-1)], 64)
			sample = &prompb.Sample{
				Value:     V,
				Timestamp: T,
			}
			tse.Samples = append(tse.Samples, *sample)
		} else if strings.Contains(line, "server.go:202:") {
			lbs = make([]*prompb.Label, 0)
			lb := strings.Split(line[45:], "{")
			label.Name = model.MetricNameLabel
			label.Value = lb[0]
			lbs = append(lbs, &label)
			lbc := strings.Split(lb[1][:len(lb[1])-1], ", ")
			for i = 0; i < len(lbc); i++ {
				content := strings.Split(lbc[i], "=\"")
				label.Name = content[0]
				if i == len(lbc)-1 {
					label.Value = content[1][:len(content[1])-2]
				} else {

					label.Value = content[1][:len(content[1])-1]
				}
				lbs = append(lbs, &label)
			}
			tse.Labels = lbs
			ts = append(ts, &tse)
			req.Timeseries = ts
		}

	}

}
