//TODO add a license

package main

import (
	"container/list"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	_ "github.com/taosdata/TDengine/src/connector/go/src/taosSql"

	"github.com/prometheus/prometheus/prompb"
)

type Bailongma struct {
}

type nametag struct {
	tagmap  map[string]string
	taglist *list.List
}

var (
	daemonUrl   string
	httpworkers int
	sqlworkers  int
	batchSize   int
	buffersize  int
	dbname      string
	dbuser      string
	dbpassword  string
	rwport      string
	debugprt    int
)

// Global vars
var (
	bufPool         sync.Pool
	batchChans      []chan string              //multi table one chan
	nodeChans       []chan prompb.WriteRequest //multi node one chan
	inputDone       chan struct{}
	workersGroup    sync.WaitGroup
	reportTags      [][2]string
	reportHostname  string
	taosDriverName  string = "taosSql"
	IsSTableCreated sync.Map
	IsTableCreated  sync.Map
)
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "host", "", "TDengine host.")

	flag.IntVar(&batchSize, "batch-size", 10, "Batch size (input items).")
	flag.IntVar(&httpworkers, "http-workers", 10, "Number of parallel http requests handler .")
	flag.IntVar(&sqlworkers, "sql-workers", 10, "Number of parallel sql handler.")
	flag.StringVar(&dbname, "dbname", "prometheus", "Database name where to store metrics")
	flag.StringVar(&dbuser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbpassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwport, "port", "10203", "remote write port")
	flag.IntVar(&debugprt, "debugprt", 0, "if 0 not print, if 1 print the sql")

	flag.Parse()
	daemonUrl = daemonUrl + ":0"
	fmt.Print("host: ")
	fmt.Print(daemonUrl)
	fmt.Print("  port: ")
	fmt.Print(rwport)
	fmt.Print("  database: ")
	fmt.Print(dbname)

}

func main() {

	for i := 0; i < httpworkers; i++ {
		nodeChans = append(nodeChans, make(chan prompb.WriteRequest, buffersize))
	}

	createDatabase(dbname)

	for i := 0; i < httpworkers; i++ {
		workersGroup.Add(1)
		go NodeProcess(i)
	}

	for i := 0; i < sqlworkers; i++ {
		batchChans = append(batchChans, make(chan string, batchSize))
	}

	for i := 0; i < sqlworkers; i++ {
		workersGroup.Add(1)
		go processBatches(i)
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		addr := strings.Split(r.RemoteAddr, ":")
		idx := TAOShashID([]byte(addr[0]))

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

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
		nodeChans[idx%httpworkers] <- req
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":"+rwport, nil))

}

func TAOShashID(ba []byte) int {
	var sum int = 0
	for i := 0; i < len(ba); i++ {
		sum += int(ba[i] - '0')
	}
	return sum
}

func NodeProcess(workerid int) error {

	for req := range nodeChans[workerid] {

		ProcessReq(req)

	}

	return nil
}

func ProcessReq(req prompb.WriteRequest) error {

	for _, ts := range req.Timeseries {
		taglist := list.New()
		tagmap := make(map[string]string)
		m := make(model.Metric, len(ts.Labels))
		var tbn string = ""
		//fmt.Println(ts)
		for _, l := range ts.Labels {
			m[model.LabelName(l.Name)] = model.LabelValue(l.Value)

			if string(l.Name) == "__name__" {
				continue
			}

			taglist.PushBack(string(l.Name))

			tagmap[string(l.Name)] = string(l.Value)
		}
		metricName, hasName := m["__name__"]
		if hasName {
			stbname := string(metricName)
			if len(stbname) >= 60 {
				//stbname = "md5_"+md5V2(stbname)
				stbname = string([]byte(stbname)[:60])
			}
			schema, ok := IsSTableCreated.Load(stbname)
			if !ok {
				var nt nametag

				nt.taglist = taglist
				nt.tagmap = tagmap
				IsSTableCreated.Store(stbname, nt)
				var sqlcmd string
				sqlcmd = "create table if not exists " + stbname + " (ts timestamp, value double) tags("
				i := 0
				for e := taglist.Front(); e != nil; e = e.Next() {
					if i == 0 {
						sqlcmd = sqlcmd + "t_" + e.Value.(string) + " binary(50)"
					} else {
						sqlcmd = sqlcmd + ",t_" + e.Value.(string) + " binary(50)"
					}
					i++
					s, _ := tagmap[e.Value.(string)]
					tbn = tbn + s
				}
				sqlcmd = sqlcmd + ")\n"
				tbn = stbname + tbn
				execSql(dbname, sqlcmd)
				SerilizeTDengine(ts, stbname, tbn, taglist, tagmap)
			} else {
				nt := schema.(nametag)
				tagmap = nt.tagmap
				taglist = nt.taglist
				var sqlcmd string
				for _, l := range ts.Labels {
					k := string(l.Name)
					if k == "__name__" {
						continue
					}
					_, ok := tagmap[k]
					if !ok {
						sqlcmd = sqlcmd + " alter table " + stbname + " add tag t_" + k + " binary(40)\n"
						taglist.PushBack(k)
						tagmap[k] = string(l.Value)
						execSql(dbname, sqlcmd)
					}
				}

				for e := taglist.Front(); e != nil; e = e.Next() {
					s, _ := tagmap[e.Value.(string)]
					tbn = tbn + s
				}
				tbn = stbname + tbn
				SerilizeTDengine(ts, stbname, tbn, taglist, tagmap)
			}
		} else {
			info := fmt.Sprintf("no name metric")
			panic(info)
		}
	}
	return nil
}

func SerilizeTDengine(m prompb.TimeSeries, stbname string, tbn string, taglist *list.List, tagmap map[string]string) error {

	s := "MD5_" + md5V2(tbn)
	/*if debugprt == 2 {
		fmt.Print(tbn)
		fmt.Print(" : ")
		fmt.Println(s)
	}*/
	_, ok := IsTableCreated.Load(s)
	if !ok {
		var sqlcmd string
		sqlcmd = "create table if not exists " + s + " using " + stbname + " tags("
		i := 0
		for e := taglist.Front(); e != nil; e = e.Next() {
			tagvalue, has := tagmap[e.Value.(string)]
			if len(tagvalue) >= 60 {
				tagvalue = tagvalue[:59]
			}

			if i == 0 {
				if has {
					sqlcmd = sqlcmd + "\"" + tagvalue + "\""
				} else {
					sqlcmd = sqlcmd + "null"
				}
				i++
			} else {
				if has {
					sqlcmd = sqlcmd + ",\"" + tagvalue + "\""
				} else {
					sqlcmd = sqlcmd + ",null"
				}
			}

		}
		sqlcmd = sqlcmd + ")\n"
		execSql(dbname, sqlcmd)
		IsTableCreated.Store(s, true)
	} else {
		idx := TAOShashID([]byte(s))
		sqlcmd := " " + s + " values("
		vl := m.Samples[0].GetValue()
		vls := strconv.FormatFloat(vl, 'E', -1, 64)

		if vls == "NaN" {
			vls = "null"
		}
		tl := m.Samples[0].GetTimestamp()
		tls := strconv.FormatInt(tl, 10)
		sqlcmd = sqlcmd + tls + "," + vls + ")"

		batchChans[idx%sqlworkers] <- sqlcmd
	}

	return nil
}

func createDatabase(dbname string) {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/")
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
	sqlcmd := fmt.Sprintf("create database if not exists %s", dbname)
	_, err = db.Exec(sqlcmd)
	sqlcmd = fmt.Sprintf("use %s", dbname)
	_, err = db.Exec(sqlcmd)
	checkErr(err)
	return
}

func execSql(dbname string, sqlcmd string) {
	if len(sqlcmd) < 1 {
		return
	}
	if debugprt == 1 {
		fmt.Println(sqlcmd)
	}
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
	_, err = db.Exec(sqlcmd)
	if err != nil {
		var count int = 2
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = db.Exec(sqlcmd)
				count--
			} else {
				if err != nil {
					log.Printf("Error: %s sqlcmd: %s\n", err, sqlcmd)
					return
				}
				break
			}

		}
	}
	return
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
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
	if err != nil {
		log.Printf("processBatches Open database error: %s\n", err)
		var count int = 5
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
				count--
			} else {
				if err != nil {
					log.Printf("Error: %s open database\n", err)
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

	for onepoint := range batchChans[iworker] {
		sqlcmd[i] = onepoint
		i++
		if i > batchSize {
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
							log.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
						}
						break
					}

				}
			}
		}
	}
	if i > 0 {
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
						log.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
					}
					break
				}
			}
		}
	}

	workersGroup.Done()
}
