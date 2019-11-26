//TODO add a license

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"database/sql"
	"flag"
	"crypto/md5"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	_ "github.com/taosdata/TDengine/src/connector/go/src/taosSql"

	"github.com/prometheus/prometheus/prompb"
)

type Bailongma struct {
}

var (
	daemonUrl      string
	httpworkers    int
	sqlworkers      int
	batchSize      int
	buffersize     int
	dbname         string
	dbuser     string
	dbpassword string
	rwport         string
)
 
// Global vars
var (
	bufPool        sync.Pool
	batchChans     []chan string        //multi table one chan
	nodeChans      []chan prompb.WriteRequest //multi node one chan
	inputDone      chan struct{}
	workersGroup   sync.WaitGroup
	reportTags     [][2]string
	reportHostname string
	taosDriverName string = "taosSql"
	IsSTableCreated      sync.Map
)
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "host", "192.168.0.4:0", "TDengine host.")
	flag.IntVar(&batchSize, "batch-size", 10, "Batch size (input items).")
	flag.IntVar(&httpworkers, "http-workers", 10, "Number of parallel http requests handler .")
	flag.IntVar(&sqlworkers, "sql-workers", 10, "Number of parallel sql handler.")
	flag.StringVar(&dbname, "dbname", "prometheus", "Database name where to store metrics")
	flag.StringVar(&dbuser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbpassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwport, "port", "10202", "remote write port")


	flag.Parse()

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

	log.Fatal(http.ListenAndServe(":"+rwport, nil))


}

func TAOSSerializeTimeseries(ts prompb.TimeSeries, stbn string,tbname string) (sqlcmd []byte, err error) {

	buf := scratchBufPool.Get().([]byte)
	s := fmt.Sprintf(" %s using %s tags(", tbname, stbn)
	buf = append(buf, s...)	
	var head int =0
	
	for _,l := range ts.Labels{
		if string(l.Name) == "__name__" {
			continue
		}
		if head ==0{
			buf = append(buf, "\""+string(l.Value)+"\""...)
			head =1
		}else {
			buf = append(buf,",\""+string(l.Value)+"\""...)
		}
	}
	buf = append(buf,") values("...)
	head = 0
	for _, s := range ts.Samples {
		if head ==0{
			var t int64
			var vl float64
			var v string 
			vl = s.GetValue()
			t = s.GetTimestamp()
			flt := strconv.FormatFloat(vl,'E',-1,64)
			if flt == "NaN" { 
				v = fmt.Sprintf("%d,null)", t)
			}else {
				v = fmt.Sprintf("%d,%f)", t, vl)
			}
	
			buf = append(buf,v...)
			head = 1
		}else {
			einfo := fmt.Sprintf("%d values, more than one value, have to redesign", len(ts.Samples))
			panic(einfo)
		}
	}
	return buf, nil	
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

func ProcessReq(req prompb.WriteRequest) error{


	for _, ts := range req.Timeseries {
		//TAOSSerializeTimeseries(ts, db)
		m := make(model.Metric, len(ts.Labels))
		var tbn string 
		for _, l := range ts.Labels {
			m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
			tbn = tbn+ string(l.Value)
		}
		metricName, hasName := m["__name__"]
		if hasName {
			stbname := string(metricName);
			if len(stbname)>=60 {
				//stbname = "md5_"+md5V2(stbname)
				stbname = string([]byte(stbname)[:60])
			}
			_,ok := IsSTableCreated.Load(stbname)
			if !ok {
				TAOSCreateStable(ts,stbname,dbname)
			}else {
				idx := TAOShashID([]byte(tbn))
				tbn = "md5_"+md5V2(tbn)
				sqlCmd,_:= TAOSSerializeTimeseries(ts,stbname,tbn)
				if sqlCmd != nil{
					batchChans[idx%sqlworkers]<- string(sqlCmd)

				}else {
					info := fmt.Sprintf("serilize faild, stbname %s",stbname)
					panic(info)
				}

			}
		}else {
			info := fmt.Sprintf("no name metric")
			panic(info)
		}
	}
	return nil
}

func TAOSCreateStable(ts prompb.TimeSeries, tname string,dbn string) error {
	
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbn)
	if err != nil {
		log.Fatalf("TAOSCreateStable Open database error: %s\n", err)
	}
	defer db.Close()	
	// assemble the create super table command line
	buf := scratchBufPool.Get().([]byte)
	s := fmt.Sprintf("create table if not exists %s (ts timestamp, value1 double) tags(", tname)
	buf = append(buf, s...)
	
	var start int = 0
	for _, l := range ts.Labels {
		if l.Name == "__name__"{
			continue
		}
		if start == 0{
			buf = append(buf, "t_"+l.Name+" binary(50)"...)
			start  = 1
		}else {
			buf = append(buf, ", t_"+l.Name+" binary(50)"...)
		}
	}
	buf = append(buf,");\n"...)
	_, err = db.Exec(string(buf))
	if err != nil {
		log.Fatalf("Error writing: %s\n", string(buf))//err.Error())
	}
	IsSTableCreated.Store(tname,true)
	//fmt.Println(string(buf))
	
	buf = buf[:0]
	scratchBufPool.Put(buf)
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

func checkErr(err error) {
	if err != nil {
		panic(err)
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
		log.Fatalf("processBatches Open database error: %s\n", err)
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
				log.Fatalf("Error writing: %s\n", strings.Join(sqlcmd, ""))//err.Error())
			}
		}
	}
	if i > 0 {
		i = 1
		_, err := db.Exec(strings.Join(sqlcmd, ""))
		if err != nil {
			log.Fatalf("Error writing: %s\n", strings.Join(sqlcmd, ""))//err.Error())
		}
	}

	workersGroup.Done()
}