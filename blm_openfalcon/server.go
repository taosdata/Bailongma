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
	"container/list"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/emirpasic/gods/sets/hashset"
	_ "github.com/taosdata/driver-go/taosSql"
)

type metric struct {
	Fields    map[string]interface{}
	Name      string
	Tags      map[string]string
	TimeStamp int64
}

type Metrics struct {
	Metrics []metric
	HostIP  string
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
	taglen      int
)

type nametag struct {
	tagmap  map[string]string
	taglist *list.List
}

// Global vars
var (
	bufPool    sync.Pool
	batchChans []chan string  //multi table one chan
	nodeChans  []chan Metrics //multi node one chan
	inputDone  chan struct{}
	//workersGroup    sync.WaitGroup
	reportTags      [][2]string
	reportHostname  string
	taosDriverName  string = "taosSql"
	IsSTableCreated sync.Map
	IsTableCreated  sync.Map
	taglist         *list.List
	nametagmap      map[string]nametag
	tagstr          string
	blmLog          *log.Logger
	logNameDefault  string = "/var/log/taos/blm_openfalcon.log"
)
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

var keywordsSet = hashset.New()

func KeywordsSetInit() {
	keywordsSet.Add("ablocks")
	keywordsSet.Add("abort")
	keywordsSet.Add("account")
	keywordsSet.Add("accounts")
	keywordsSet.Add("add")
	keywordsSet.Add("after")
	keywordsSet.Add("all")
	keywordsSet.Add("alter")
	keywordsSet.Add("and")
	keywordsSet.Add("as")
	keywordsSet.Add("asc")
	keywordsSet.Add("attach")
	keywordsSet.Add("avg")
	keywordsSet.Add("before")
	keywordsSet.Add("begin")
	keywordsSet.Add("between")
	keywordsSet.Add("bigint")
	keywordsSet.Add("binary")
	keywordsSet.Add("bitand")
	keywordsSet.Add("bitnot")
	keywordsSet.Add("bitor")
	keywordsSet.Add("bool")
	keywordsSet.Add("bottom")
	keywordsSet.Add("by")
	keywordsSet.Add("cache")
	keywordsSet.Add("cascade")
	keywordsSet.Add("change")
	keywordsSet.Add("clog")
	keywordsSet.Add("cluster")
	keywordsSet.Add("colon")
	keywordsSet.Add("column")
	keywordsSet.Add("comma")
	keywordsSet.Add("comp")
	keywordsSet.Add("concat")
	keywordsSet.Add("configs")
	keywordsSet.Add("conflict")
	keywordsSet.Add("connection")
	keywordsSet.Add("connections")
	keywordsSet.Add("copy")
	keywordsSet.Add("count")
	keywordsSet.Add("create")
	keywordsSet.Add("ctime")
	keywordsSet.Add("database")
	keywordsSet.Add("databases")
	keywordsSet.Add("days")
	keywordsSet.Add("deferred")
	keywordsSet.Add("delimiters")
	keywordsSet.Add("desc")
	keywordsSet.Add("describe")
	keywordsSet.Add("detach")
	keywordsSet.Add("diff")
	keywordsSet.Add("distinct")
	keywordsSet.Add("divide")
	keywordsSet.Add("dnode")
	keywordsSet.Add("dnodes")
	keywordsSet.Add("dot")
	keywordsSet.Add("double")
	keywordsSet.Add("drop")
	keywordsSet.Add("each")
	keywordsSet.Add("end")
	keywordsSet.Add("eq")
	keywordsSet.Add("exists")
	keywordsSet.Add("explain")
	keywordsSet.Add("fail")
	keywordsSet.Add("fill")
	keywordsSet.Add("first")
	keywordsSet.Add("float")
	keywordsSet.Add("for")
	keywordsSet.Add("from")
	keywordsSet.Add("ge")
	keywordsSet.Add("glob")
	keywordsSet.Add("grants")
	keywordsSet.Add("group")
	keywordsSet.Add("gt")
	keywordsSet.Add("having")
	keywordsSet.Add("id")
	keywordsSet.Add("if")
	keywordsSet.Add("ignore")
	keywordsSet.Add("immediate")
	keywordsSet.Add("import")
	keywordsSet.Add("in")
	keywordsSet.Add("initially")
	keywordsSet.Add("insert")
	keywordsSet.Add("instead")
	keywordsSet.Add("integer")
	keywordsSet.Add("interval")
	keywordsSet.Add("into")
	keywordsSet.Add("ip")
	keywordsSet.Add("is")
	keywordsSet.Add("isnull")
	keywordsSet.Add("join")
	keywordsSet.Add("keep")
	keywordsSet.Add("key")
	keywordsSet.Add("kill")
	keywordsSet.Add("last")
	keywordsSet.Add("le")
	keywordsSet.Add("leastsquares")
	keywordsSet.Add("like")
	keywordsSet.Add("limit")
	keywordsSet.Add("linear")
	keywordsSet.Add("local")
	keywordsSet.Add("lp")
	keywordsSet.Add("lshift")
	keywordsSet.Add("lt")
	keywordsSet.Add("match")
	keywordsSet.Add("max")
	keywordsSet.Add("metric")
	keywordsSet.Add("metrics")
	keywordsSet.Add("min")
	keywordsSet.Add("minus")
	keywordsSet.Add("mnodes")
	keywordsSet.Add("modules")
	keywordsSet.Add("nchar")
	keywordsSet.Add("ne")
	keywordsSet.Add("none")
	keywordsSet.Add("not")
	keywordsSet.Add("notnull")
	keywordsSet.Add("now")
	keywordsSet.Add("of")
	keywordsSet.Add("offset")
	keywordsSet.Add("or")
	keywordsSet.Add("order")
	keywordsSet.Add("pass")
	keywordsSet.Add("percentile")
	keywordsSet.Add("plus")
	keywordsSet.Add("pragma")
	keywordsSet.Add("prev")
	keywordsSet.Add("privilege")
	keywordsSet.Add("queries")
	keywordsSet.Add("query")
	keywordsSet.Add("raise")
	keywordsSet.Add("rem")
	keywordsSet.Add("replace")
	keywordsSet.Add("replica")
	keywordsSet.Add("reset")
	keywordsSet.Add("restrict")
	keywordsSet.Add("row")
	keywordsSet.Add("rows")
	keywordsSet.Add("rp")
	keywordsSet.Add("rshift")
	keywordsSet.Add("scores")
	keywordsSet.Add("select")
	keywordsSet.Add("semi")
	keywordsSet.Add("set")
	keywordsSet.Add("show")
	keywordsSet.Add("slash")
	keywordsSet.Add("sliding")
	keywordsSet.Add("slimit")
	keywordsSet.Add("smallint")
	keywordsSet.Add("spread")
	keywordsSet.Add("stable")
	keywordsSet.Add("stables")
	keywordsSet.Add("star")
	keywordsSet.Add("statement")
	keywordsSet.Add("stddev")
	keywordsSet.Add("stream")
	keywordsSet.Add("streams")
	keywordsSet.Add("string")
	keywordsSet.Add("sum")
	keywordsSet.Add("table")
	keywordsSet.Add("tables")
	keywordsSet.Add("tag")
	keywordsSet.Add("tags")
	keywordsSet.Add("tblocks")
	keywordsSet.Add("tbname")
	keywordsSet.Add("times")
	keywordsSet.Add("timestamp")
	keywordsSet.Add("tinyint")
	keywordsSet.Add("top")
	keywordsSet.Add("topic")
	keywordsSet.Add("trigger")
	keywordsSet.Add("uminus")
	keywordsSet.Add("uplus")
	keywordsSet.Add("use")
	keywordsSet.Add("user")
	keywordsSet.Add("users")
	keywordsSet.Add("using")
	keywordsSet.Add("values")
	keywordsSet.Add("variable")
	keywordsSet.Add("vgroups")
	keywordsSet.Add("view")
	keywordsSet.Add("wavg")
	keywordsSet.Add("where")
}

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "host", "127.0.0.1", "TDengine host.")

	flag.IntVar(&batchSize, "batch-size", 10, "Batch size (input items).")
	flag.IntVar(&httpworkers, "http-workers", 10, "Number of parallel http requests handler .")
	flag.IntVar(&sqlworkers, "sql-workers", 10, "Number of parallel sql handler.")
	flag.StringVar(&dbname, "dbname", "openfalcon", "Database name where to store metrics")
	flag.StringVar(&dbuser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbpassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwport, "port", "10202", "remote write port")
	flag.IntVar(&debugprt, "debugprt", 0, "if 0 not print, if 1 print the sql")
	flag.IntVar(&taglen, "tag-length", 30, "the max length of tag string")
	flag.IntVar(&buffersize, "buffersize", 100, "the buffer size of metrics received")

	flag.Parse()
	daemonUrl = daemonUrl + ":0"
	nametagmap = make(map[string]nametag)
	fmt.Print("host: ")
	fmt.Print(daemonUrl)
	fmt.Print("  port: ")
	fmt.Print(rwport)
	fmt.Print("  database: ")
	fmt.Print(dbname)
	tagstr = fmt.Sprintf(" binary(%d)", taglen)
	logFile, err := os.OpenFile(logNameDefault, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	blmLog = log.New(logFile, "", log.LstdFlags)
	blmLog.SetPrefix("BLM_TLG")
	blmLog.SetFlags(log.LstdFlags | log.Lshortfile)

	KeywordsSetInit()

}

func main() {

	for i := 0; i < httpworkers; i++ {
		nodeChans = append(nodeChans, make(chan Metrics, buffersize))
	}

	createDatabase(dbname)

	for i := 0; i < httpworkers; i++ {
		//workersGroup.Add(1)
		go NodeProcess(i)
	}

	for i := 0; i < sqlworkers; i++ {
		batchChans = append(batchChans, make(chan string, batchSize))
	}

	for i := 0; i < sqlworkers; i++ {
		//workersGroup.Add(1)
		go processBatches(i)
	}

	http.HandleFunc("/openfalcon", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		addr := strings.Split(r.RemoteAddr, ":")
		idx := TAOShashID([]byte(addr[0]))

		reqBuf, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		var req Metrics
		if err := json.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req.HostIP = addr[0]

		nodeChans[idx%httpworkers] <- req

	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	blmLog.Fatal(http.ListenAndServe(":"+rwport, nil))

}

func TAOShashID(ba []byte) int {
	var sum int = 0

	for i := 0; i < len(ba); i++ {
		sum += int(ba[i] - '0')
	}

	return sum
}

func TAOSstrCmp(a string, b string) bool {
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

func NodeProcess(workerid int) error {

	for req := range nodeChans[workerid] {

		ProcessReq(req)

	}

	return nil
}

func OrderInsert(ts int64, l *list.List) {
	e := l.Front()
	if e == nil {
		l.PushFront(ts)
		return
	}

	for e = l.Front(); e != nil; e = e.Next() {

		if e.Value.(int64) < ts {
			continue
		} else {
			l.InsertBefore(ts, e)
			return
		}
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

		if TAOSstrCmp(str, s) {
			continue
		} else {
			l.InsertBefore(s, e)
			return
		}
	}
	l.PushBack(s)
	return
}

func ProcessReq(req Metrics) error {

	tsmap := make(map[int64]map[string][]metric)
	tslist := list.New()
	addr := req.HostIP

	var lastTs int64 = 0
	for i := 0; i < len(req.Metrics); i++ {
		m := req.Metrics[i]
		if tsmap[m.TimeStamp] == nil {
			tsmap[m.TimeStamp] = make(map[string][]metric)
		}

		mp := tsmap[m.TimeStamp]
		mp[m.Name] = append(mp[m.Name], m)

		if lastTs != m.TimeStamp { //there is still some case that will make mistake, when the timestamp is totally out of order. but right now just forget it.
			OrderInsert(m.TimeStamp, tslist)
		}
		lastTs = m.TimeStamp
	}

	for e := tslist.Front(); e != nil; e = e.Next() {

		namemap, ok := tsmap[e.Value.(int64)]
		if ok {
			for _, v := range namemap {

				ProcessData(v, dbname, addr)
			}
		} else {
			info := fmt.Sprintf("ProcessReq: cannot retrieve map")
			panic(info)
		}

	}

	return nil
}

func SerilizeTDengine(m metric, dbn string, hostip string, taglist *list.List, db *sql.DB) error {
	var tbna []string

	for _, v := range m.Tags {
		tbna = append(tbna, v)
	}
	sort.Strings(tbna)
	tbn := strings.Join(tbna, "") // Go map 遍历结果是随机的，必须排下序

	for k, v := range m.Fields {
		s := m.Name + tbn + hostip + k
		//fmt.Print(s)
		s = "MD5_" + md5V2(s)
		_, ok := IsTableCreated.Load(s)
		if !ok {
			var sqlcmd string
			switch v.(type) {
			case string:
				sqlcmd = "create table if not exists " + s + " using " + m.Name + "_str tags("
			default:
				sqlcmd = "create table if not exists " + s + " using " + m.Name + " tags("
			}

			for e := taglist.Front(); e != nil; e = e.Next() {
				tagvalue, has := m.Tags[e.Value.(string)]
				if len(tagvalue) >= 60 {
					tagvalue = tagvalue[:59]
				}
				if has {
					sqlcmd = sqlcmd + "\"" + tagvalue + "\","
				} else {
					sqlcmd = sqlcmd + "null,"
				}
			}
			sqlcmd = sqlcmd + "\"" + hostip + "\"," + "\"" + k + "\")\n"
			execSql(dbn, sqlcmd, db)
			IsTableCreated.Store(s, true)
		}
		idx := TAOShashID([]byte(s))
		sqlcmd := " " + s + " values("

		tls := strconv.FormatInt(m.TimeStamp, 10)
		switch v.(type) {
		case string:
			sqlcmd = sqlcmd + tls + ",\"" + v.(string) + "\")"
		case int64:
			sqlcmd = sqlcmd + tls + "," + strconv.FormatInt(v.(int64), 10) + ")"
		case float64:
			sqlcmd = sqlcmd + tls + "," + strconv.FormatFloat(v.(float64), 'E', -1, 64) + ")"
		default:
			panic("Checktable error value type")
		}
		batchChans[idx%sqlworkers] <- sqlcmd
		//execSql(dbn,sqlcmd)
	}
	return nil
}

func TaosNameEscape(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, ":", "_")
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "-", "_")
	chars := []rune(name)
	if len(name) == 0 {
		return "__"
	}
	if unicode.IsDigit(chars[0]) {
		return "_" + name
	}
	if keywordsSet.Contains(name) {
		return "_" + name
	}
	return name
}
func TaosTableNameEscape(name string) string {
	name = TaosNameEscape(name)
	if len(name) > 160 {
		return name[:160]
	}
	return name
}
func TaosFieldNameEscape(name string) string {
	name = TaosNameEscape(name)
	if len(name) > 64 {
		return name[:64]
	}
	return name
}

func ProcessData(ts []metric, dbn string, hostip string) error {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
	if err != nil {
		blmLog.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
	schema, ok := IsSTableCreated.Load(ts[0].Name)

	if !ok {
		var nt nametag
		nt.taglist = list.New()
		nt.tagmap = make(map[string]string)
		IsSTableCreated.Store(ts[0].Name, nt)
		tagmap := nt.tagmap
		taglist := nt.taglist

		for i := 0; i < len(ts); i++ {

			for k, _ := range ts[i].Tags {
				k = TaosFieldNameEscape(k)
				_, ok := tagmap[k]
				if !ok {
					taglist.PushBack(k)
					tagmap[k] = "y"
				}
			}
		}
		var sqlcmd string
		tbname := TaosTableNameEscape(ts[0].Name)
		sqlcmd = "create table if not exists " + tbname + " (ts timestamp, value double) tags("
		sqlcmd1 := "create table if not exists " + tbname + "_str (ts timestamp, value binary(256)) tags("
		for e := taglist.Front(); e != nil; e = e.Next() {
			tagname := e.Value.(string)
			sqlcmd = sqlcmd + tagname + tagstr + ","
			sqlcmd1 = sqlcmd1 + tagname + tagstr + ","
		}
		sqlcmd = sqlcmd + "srcip binary(20), field binary(40))\n"
		sqlcmd1 = sqlcmd1 + "srcip binary(20), field binary(40))\n"
		execSql(dbn, sqlcmd, db)
		execSql(dbn, sqlcmd1, db)
		for i := 0; i < len(ts); i++ {
			SerilizeTDengine(ts[i], dbn, hostip, taglist, db)
		}

		return nil
	}
	nt := schema.(nametag)
	tagmap := nt.tagmap
	taglist := nt.taglist

	var sqlcmd string
	for i := 0; i < len(ts); i++ {
		for k, _ := range ts[i].Tags {
			k = TaosFieldNameEscape(k)
			_, ok := tagmap[k]
			if !ok {
				tbname := TaosTableNameEscape(ts[0].Name)
				sqlcmd = sqlcmd + "alter table " + tbname + " add tag " + k + tagstr + "\n"
				sqlcmd = sqlcmd + "alter table " + tbname + "_str add tag " + k + tagstr + "\n"
				taglist.PushBack(k)
				tagmap[k] = "y"

			}
		}

	}
	sqls := strings.Split(sqlcmd, "\n")
	for _, s := range sqls {
		execSql(dbn, s, db)
	}

	for i := 0; i < len(ts); i++ {
		SerilizeTDengine(ts[i], dbn, hostip, taglist, db)
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

func execSql(dbname string, sqlcmd string, db *sql.DB) {
	if len(sqlcmd) < 1 {
		return
	}
	_, err := db.Exec(sqlcmd)
	if err != nil {
		var count int = 2
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = db.Exec(sqlcmd)
				count--
			} else {
				if err != nil {
					blmLog.Printf("execSql Error: %s sqlcmd: %s\n", err, sqlcmd)
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
		blmLog.Println(err)
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
		blmLog.Printf("processBatches Open database error: %s\n", err)
		var count int = 5
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
				count--
			} else {
				if err != nil {
					blmLog.Printf("processBatches Error: %s open database\n", err)
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
							blmLog.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
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
						blmLog.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
					}
					break
				}
			}
		}
	}

	//workersGroup.Done()
}
