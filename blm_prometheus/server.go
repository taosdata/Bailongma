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

package main

import (
	"blm_prometheus/pkg/tdengine"
	"bufio"
	"container/list"
	"crypto/md5"
	"database/sql"
	"encoding/json"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	blmLog "blm_prometheus/pkg/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	_ "github.com/taosdata/driver-go/taosSql"
)

type nameTag struct {
	tagMap  map[string]string
	tagList *list.List
}

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
	HealthCheck() error
}

var (
	daemonIP      string
	daemonName    string
	httpWorkers   int
	sqlWorkers    int
	batchSize     int
	bufferSize    int
	dbName        string
	dbUser        string
	dbPassword    string
	rwPort        string
	apiPort       string
	debugPrt      int
	tagLen        int
	tagLimit      int = 1024
	tagNumLimit   int
	tablePervNode int
)

// Global vars
var (
	bufPool         sync.Pool
	batchChannels   []chan string              //multi table one chan
	nodeChannels    []chan prompb.WriteRequest //multi node one chan
	inputDone       chan struct{}
	workersGroup    sync.WaitGroup
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
	flag.StringVar(&daemonIP, "tdengine-ip", "127.0.0.1", "TDengine host IP.")
	flag.StringVar(&daemonName, "tdengine-name", "", "TDengine host Name. in K8S, could be used to lookup TDengine's IP")
	flag.StringVar(&apiPort, "tdengine-api-port", "6041", "TDengine restful API port")
	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&httpWorkers, "http-workers", 1, "Number of parallel http requests handler .")
	flag.IntVar(&sqlWorkers, "sql-workers", 1, "Number of parallel sql handler.")
	flag.StringVar(&dbName, "dbname", "prometheus", "Database name where to store metrics")
	flag.StringVar(&dbUser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbPassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwPort, "port", "10203", "remote write port")
	flag.IntVar(&debugPrt, "debugprt", 0, "if 0 not print, if 1 print the sql")
	flag.IntVar(&tagLen, "tag-length", 128, "the max length of tag string,default is 30")
	flag.IntVar(&bufferSize, "buffersize", 100, "the buffer size of metrics received")
	flag.IntVar(&tagNumLimit, "tag-num", 128, "the number of tags in a super table, default is 8")
	//flag.IntVar(&tablepervnode, "table-num", 10000, "the number of tables per TDengine Vnode can create, default 10000")

	flag.Parse()

	if daemonName != "" {
		// look DNS A records
		s, _ := net.LookupIP(daemonName)
		daemonIP = fmt.Sprintf("%s", s[0])
		daemonIP = daemonIP + ":0"
		fmt.Println("daemonIP:" + daemonIP)

		tdUrl = daemonName
	} else {
		tdUrl = daemonIP
		daemonIP = daemonIP + ":0"
	}

	tagStr = fmt.Sprintf(" binary(%d)", tagLen)
	// init logger
	blmLog.Init(logNameDefault)

	log.Printf("host: ip")
	log.Printf(daemonIP)
	log.Printf("  port: ")
	log.Printf(rwPort)
	log.Printf("  database: ")
	log.Println(dbName)

}

func main() {

	for i := 0; i < httpWorkers; i++ {
		nodeChannels = append(nodeChannels, make(chan prompb.WriteRequest, bufferSize))
	}

	createDatabase(dbName)

	for i := 0; i < httpWorkers; i++ {
		workersGroup.Add(1)
		go NodeProcess(i)
	}

	for i := 0; i < sqlWorkers; i++ {
		batchChannels = append(batchChannels, make(chan string, batchSize))
	}

	for i := 0; i < sqlWorkers; i++ {
		workersGroup.Add(1)
		go processBatches(i)
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
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
		nodeChannels[idx%httpWorkers] <- req
	})
	http.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		//log.Println(string(compressed))
		var output string = ""
		schema, ok := IsSTableCreated.Load(string(compressed))
		if !ok {
			output = "the stable is not created!"
		} else {
			nTag := schema.(nameTag)
			tbTagList := nTag.tagList
			tbTagMap := nTag.tagMap
			output = "tags: "
			for e := tbTagList.Front(); e != nil; e = e.Next() {
				output = output + e.Value.(string) + " | "
			}
			output = output + "\ntbTagMap: "
			s := fmt.Sprintln(tbTagMap)
			output = output + s
		}

		res := queryTableStruct(string(compressed))
		output = output + "\nTable structure:\n" + res
		s := fmt.Sprintf("query result:\n %s\n", output)
		//log.Println(s)
		w.Write([]byte(s))
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	if debugPrt == 5 {
		TestSerialization()
	}
	// read
	http.Handle("/read", readHandle())
	log.Fatal(http.ListenAndServe(":"+rwPort, nil))

}

func buildClients() reader {
	config := tdengine.Config{
		DbName:     dbName,
		DbPassword: dbPassword,
		DbUser:     dbUser,
		DaemonIP:   daemonIP,
		DaemonName: daemonName,
		Table:      "",
	}
	tdClient := tdengine.NewClient(&config)
	return tdClient
}

func readHandle() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reader := buildClients()
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			//log.Fatalf("Read error: %s\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Printf("Decode error: %s\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Printf("Unmarshal error: %s\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("req info== %v\n", req)
		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			log.Printf("msg", "Error executing query", "query", req, "storage", reader.Name(), "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}
func queryTableStruct(tbname string) string {
	client := new(http.Client)
	s := fmt.Sprintf("describe %s.%s", dbName, tbname)
	body := strings.NewReader(s)
	req, _ := http.NewRequest("GET", "http://"+tdUrl+":"+apiPort+"/rest/sql", body)
	//fmt.Println("http://" + tdurl + ":" + apiPort + "/rest/sql" + s)
	req.SetBasicAuth(dbUser, dbPassword)
	resp, err := client.Do(req)

	if err != nil {
		log.Println(err)
		fmt.Println(err)
		return ""
	} else {
		compressed, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return string(compressed)
	}
}

func TAOSHashID(ba []byte) int {
	var sum int = 0
	for i := 0; i < len(ba); i++ {
		sum += int(ba[i] - '0')
	}
	return sum
}

func NodeProcess(workerid int) error {
	for req := range nodeChannels[workerid] {
		ProcessReq(req)
	}
	return nil
}

func ProcessReq(req prompb.WriteRequest) error {
	db, err := sql.Open(taosDriverName, dbUser+":"+dbPassword+"@/tcp("+daemonIP+")/"+dbName)
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()

	for _, ts := range req.Timeseries {
		err = HandleStable(ts, db)
	}
	return err
}

func HandleStable(ts *prompb.TimeSeries, db *sql.DB) error {
	tagList := list.New()
	tbTagList := list.New()
	tagMap := make(map[string]string)
	tbTagMap := make(map[string]string)
	m := make(model.Metric, len(ts.Labels))
	tagNum := tagNumLimit
	var hasName bool = false
	var metricsName string
	var tbn string = ""
	var nt nameTag

	j := 0
	for _, l := range ts.Labels {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)

		if string(l.Name) == model.MetricNameLabel {
			metricsName = string(l.Value)
			tbn += metricsName
			hasName = true
			continue
		}
		j++
		ln := strings.ToLower(string(l.Name))
		OrderInsertS(ln, tagList)
		//tagList.PushBack(ln)
		s := string(l.Value)
		tbn += s
		//tagHash += s
		if j <= tagNum {
			OrderInsertS(ln, tbTagList)
			//tbTagList.PushBack(ln)
			if len(s) > tagLen {
				s = s[:tagLen]
			}
			tbTagMap[ln] = "y"
		}
		tagMap[ln] = s
	}

	if debugPrt == 2 {
		t := ts.Samples[0].Timestamp
		var ns int64 = 0
		if t/1000000000 > 10 {
			tm := t / 1000
			ns = t - tm*1000
		}
		log.Printf(" Ts: %s, value: %f, ", time.Unix(t/1000, ns), ts.Samples[0].Value)
		log.Println(ts)
	}

	if !hasName {
		info := fmt.Sprintf("no name metric")
		panic(info)
	}
	sTableName := tableNameEscape(metricsName)
	var ok bool
	schema, ok := IsSTableCreated.Load(sTableName)
	if !ok { // no local record of super table structure
		nt.tagList = tbTagList
		nt.tagMap = tbTagMap
		stbDescription := queryTableStruct(sTableName) //query the super table from TDengine
		var stbSt map[string]interface{}
		err := json.Unmarshal([]byte(stbDescription), &stbSt)
		if err == nil { //query tdengine table success!
			status := stbSt["status"].(string)
			if status == "succ" { //yes, the super table was already created in TDengine
				taostaglist := list.New()
				taostagmap := make(map[string]string)
				dt := stbSt["data"]
				for _, fd := range dt.([]interface{}) {
					fdc := fd.([]interface{})
					if fdc[3].(string) == "tag" && fdc[0].(string) != "taghash" {
						tmpstr := fdc[0].(string)
						taostaglist.PushBack(tmpstr[2:])
						taostagmap[tmpstr[2:]] = "y"
					}
				}
				nt.tagList = taostaglist
				nt.tagMap = taostagmap
				tbTagList = nt.tagList
				tbTagMap = nt.tagMap
				var sqlcmd string
				i := 0
				for _, l := range ts.Labels {
					k := strings.ToLower(string(l.Name))
					if k == model.MetricNameLabel {
						continue
					}
					i++
					if i < tagNumLimit {
						_, ok := tbTagMap[k]
						if !ok {
							sqlcmd = "alter table " + sTableName + " add tag t_" + k + tagStr + "\n"
							_, err := execSql(dbName, sqlcmd, db)
							if err != nil {
								log.Println(err)
								errorCode := fmt.Sprintf("%s", err)
								if strings.Contains(errorCode, "duplicated column names") {
									tbTagList.PushBack(k)
									//OrderInsertS(k, tbTagList)
									tbTagMap[k] = "y"
								}
							} else {
								tbTagList.PushBack(k)
								//OrderInsertS(k, tbTagList)
								tbTagMap[k] = "y"
							}
						}
					}
				}
				IsSTableCreated.Store(sTableName, nt)
			} else { // no, the super table haven't been created in TDengine, create it.
				var sqlcmd string
				sqlcmd = "create table if not exists " + sTableName + " (ts timestamp, value double) tags(taghash binary(34)"
				for e := tbTagList.Front(); e != nil; e = e.Next() {
					sqlcmd = sqlcmd + ",t_" + e.Value.(string) + tagStr
				}
				//annotlen = taglimit - i*taglen
				//nt.annotlen = annotlen
				//annotationstr := fmt.Sprintf(" binary(%d)", annotlen)
				//sqlcmd = sqlcmd + ", annotation " + annotationstr + ")\n"
				sqlcmd = sqlcmd + ")\n"
				_, err := execSql(dbName, sqlcmd, db)
				if err == nil {
					IsSTableCreated.Store(sTableName, nt)
				} else {
					log.Println(err)
				}
			}
		} else { //query TDengine table error
			log.Println(err)
		}
	} else {
		nTag := schema.(nameTag)
		tbTagList = nTag.tagList
		tbTagMap = nTag.tagMap
		var sqlcmd string
		i := 0
		for _, l := range ts.Labels {
			k := strings.ToLower(string(l.Name))
			if k == model.MetricNameLabel {
				continue
			}
			i++
			if i < tagNumLimit {
				_, ok := tbTagMap[k]
				if !ok {
					sqlcmd = "alter table " + sTableName + " add tag t_" + k + tagStr + "\n"
					_, err := execSql(dbName, sqlcmd, db)
					if err != nil {
						log.Println(err)
						errorCode := fmt.Sprintf("%s", err)
						if strings.Contains(errorCode, "duplicated column names") {
							tbTagList.PushBack(k)
							//OrderInsertS(k, tbTagList)
							tbTagMap[k] = "y"
						}
					} else {
						tbTagList.PushBack(k)
						//OrderInsertS(k, tbTagList)
						tbTagMap[k] = "y"
					}
				}
			}
		}
	}

	tbnhash := "MD5_" + md5V2(tbn)
	_, tbcreated := IsTableCreated.Load(tbnhash)

	if !tbcreated {
		var sqlcmdhead, sqlcmd string
		sqlcmdhead = "create table if not exists " + tbnhash + " using " + sTableName + " tags(\""
		sqlcmd = ""
		i := 0
		for e := tbTagList.Front(); e != nil; e = e.Next() {
			tagValue, has := tagMap[e.Value.(string)]
			if len(tagValue) > tagLen {
				tagValue = tagValue[:tagLen]
			}
			if i == 0 {
				if has {
					sqlcmd = sqlcmd + "\"" + tagValue + "\""
				} else {
					sqlcmd = sqlcmd + "null"
				}
				i++
			} else {
				if has {
					sqlcmd = sqlcmd + ",\"" + tagValue + "\""
				} else {
					sqlcmd = sqlcmd + ",null"
				}
			}
		}
		var keys []string
		var tagHash = ""
		for t := range tagMap {
			keys = append(keys, t)
		}
		sort.Strings(keys)
		for _, k := range keys {
			tagHash += tagMap[k]
		}

		sqlcmd = sqlcmd + ")\n"
		sqlcmd = sqlcmdhead + md5V2(tagHash) + "\"," + sqlcmd
		_, err := execSql(dbName, sqlcmd, db)
		if err == nil {
			IsTableCreated.Store(tbnhash, true)
		}
	}
	serializeTDengine(ts, tbnhash, db)
	return nil
}

func tableNameEscape(name string) string {
	replaceColon := strings.ReplaceAll(name, ":", "_") // replace : in the metrics name to adapt the TDengine
	replaceComma := strings.ReplaceAll(replaceColon, ".", "_")
	stbName := strings.ReplaceAll(replaceComma, "-", "_")
	if len(stbName) > 190 {
		stbName = stbName[:190]
	}
	return stbName
}

func serializeTDengine(m *prompb.TimeSeries, tbn string, db *sql.DB) error {
	idx := TAOSHashID([]byte(tbn))
	sqlcmd := " " + tbn + " values("
	vl := m.Samples[0].GetValue()
	vls := strconv.FormatFloat(vl, 'E', -1, 64)

	if vls == "NaN" {
		vls = "null"
	}
	tl := m.Samples[0].GetTimestamp()
	tls := strconv.FormatInt(tl, 10)
	sqlcmd = sqlcmd + tls + "," + vls + ")\n"
	batchChannels[idx%sqlWorkers] <- sqlcmd
	return nil
}

func createDatabase(dbName string) {
	db, err := sql.Open(taosDriverName, dbUser+":"+dbPassword+"@/tcp("+daemonIP+")/")
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

func execSql(dbName string, sqlcmd string, db *sql.DB) (sql.Result, error) {
	if len(sqlcmd) < 1 {
		return nil, nil
	}
	if debugPrt == 2 {
		log.Println(sqlcmd)
	}
	res, err := db.Exec(sqlcmd)
	if err != nil {
		log.Printf("execSql Error: %s sqlcmd: %s\n", err, sqlcmd)

	}
	return res, err
}

func checkErr(err error) {
	if err != nil {
		log.Println(err)
	}
}

func md5V2(str string) string {
	data := []byte(str)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	return md5str
}

func processBatches(iworker int) {
	var i int
	db, err := sql.Open(taosDriverName, dbUser+":"+dbPassword+"@/tcp("+daemonIP+")/"+dbName)
	if err != nil {
		log.Printf("processBatches Open database error: %s\n", err)
		var count int = 5
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = sql.Open(taosDriverName, dbUser+":"+dbPassword+"@/tcp("+daemonIP+")/"+dbName)
				count--
			} else {
				if err != nil {
					log.Printf("processBatches Error: %s open database\n", err)
					return
				}
				break
			}
		}
	}
	defer db.Close()
	sqlcmd := make([]string, batchSize+1)
	i = 0
	sqlcmd[i] = "Insert into"
	i++
	//log.Printf("processBatches")
	for onepoint := range batchChannels[iworker] {
		sqlcmd[i] = onepoint
		i++
		if i > batchSize {
			i = 1
			//log.Printf(strings.Join(sqlcmd, ""))
			_, err := db.Exec(strings.Join(sqlcmd, ""))
			if err != nil {
				log.Printf("processBatches error %s", err)
				var count int = 2
				for {
					if err != nil && count > 0 {
						<-time.After(time.Second * 1)
						_, err = db.Exec(strings.Join(sqlcmd, ""))
						count--
					} else {
						if err != nil {
							log.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
						}
						break
					}

				}
			}
		}
	}
	if i > 1 {
		i = 1
		//		log.Printf(strings.Join(sqlcmd, ""))
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
						log.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
					}
					break
				}
			}
		}
	}

	workersGroup.Done()
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
	testfile, err := os.OpenFile(promPath, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open file error!", err)
		return
	}
	defer testfile.Close()
	fmt.Println(promPath)
	buf := bufio.NewReader(testfile)
	i := 0
	lasttime := "20:40:20"
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
			if sa[3] != lasttime {
				nodeChannels[0] <- req
				lasttime = sa[3]
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

func TaosStrCmp(a string, b string) bool {
	//return if a locates before b in a dictrionary.
	for i := 0; i < len(a) && i < len(b); i++ {
		if int(a[i]-'0') > int(b[i]-'0') {
			return false
		} else if int(a[i]-'0') < int(b[i]-'0') {
			return true
		}
	}
	if len(a) > len(b) {
		return false
	} else {
		return true
	}
}

func OrderInsertS(s string, l *list.List) {
	e := l.Front()
	if e == nil {
		l.PushFront(s)
		return
	}

	for e = l.Front(); e != nil; e = e.Next() {
		str := e.Value.(string)

		if TaosStrCmp(str, s) {
			continue
		} else {
			l.InsertBefore(s, e)
			return
		}
	}
	l.PushBack(s)
	return
}
