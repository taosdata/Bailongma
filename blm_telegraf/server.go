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
	"encoding/json"
//	"sort"
	"container/list"

	_ "github.com/taosdata/TDengine/src/connector/go/src/taosSql"

	"github.com/prometheus/prometheus/prompb"
)

type metric struct {
	Fields map[string] interface{}
	Name   string	
	Tags   map[string] string 	
	TimeStamp  int64
}

type tdschema struct {
	StbName string
	Tags    *list.List
	Values  *list.List
	MultiMetric  bool
}

type tdpoint struct {
	StbName string
	Tags    *list.List
	Value   int64
}

type Metrics struct{
	Metrics []metric
	HostIP   string
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
type nametag struct {
	tagmap map[string]string 
	taglist *list.List
}
 
// Global vars
var (
	bufPool        sync.Pool
	batchChans     []chan string        //multi table one chan
	nodeChans      []chan Metrics //multi node one chan
	inputDone      chan struct{}
	workersGroup   sync.WaitGroup
	reportTags     [][2]string
	reportHostname string
	taosDriverName string = "taosSql"
	IsSTableCreated      sync.Map
	IsTableCreated      sync.Map
	taglist        *list.List
	//tagmap         map[string]string
	nametagmap     map[string]nametag
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
	flag.StringVar(&dbname, "dbname", "telegraf", "Database name where to store metrics")
	flag.StringVar(&dbuser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbpassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwport, "port", "10202", "remote write port")


	flag.Parse()

	//taglist  = list.New();
	//tagmap   = make(map[string]string)
	nametagmap = make(map[string]nametag)

}


func main() {

	for i := 0; i < httpworkers; i++ {
		nodeChans = append(nodeChans, make(chan Metrics, buffersize))
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

	http.HandleFunc("/telegraf", func(w http.ResponseWriter, r *http.Request) {
		addr := strings.Split(r.RemoteAddr, ":")
		idx := TAOShashID([]byte(addr[0]))

		reqBuf, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var req Metrics
		if err := json.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req.HostIP = addr[0]

		nodeChans[idx%httpworkers] <- req
	})

	log.Fatal(http.ListenAndServe(":"+rwport, nil))


}

func TAOSSerializeTimeseries(ts prompb.TimeSeries, stbn string,tbname string) (sqlcmd []byte, err error) {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
	if err != nil {
		log.Fatalf("TAOSSerializeTimeseries Open database error: %s\n", err)
	}
	defer db.Close()	
	// assemble the create super table command line
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

func TAOSstrCmp(a string , b string) bool {
	//return if a locates before b in a dictrionary.
	for i:=0; i<len(a) && i<len(b); i++ {
		if int(a[i]-'0') > int(b[i]-'0') {
			return false
		}else if int(a[i]-'0') < int(b[i]-'0') {
			return true
		}
	}
	if len(a)>len(b) {
		return false
	}else {
		return true
	}
}

func NodeProcess(workerid int) error {
	
	for req := range nodeChans[workerid] {

		ProcessReq(req)

	}

	return nil
}

func OrderInsert(ts int64,l *list.List) {
	e:= l.Front()
	if e == nil {
		l.PushFront(ts)
		return 
	}

	for e = l.Front(); e != nil; e = e.Next()  {
		
		if e.Value.(int64) < ts{
			continue
		}else {
			l.InsertBefore(ts,e)
			return
		}
	}
}

func OrderInsertS(s string,l *list.List) {
	e:= l.Front()
	if e == nil {
		l.PushFront(s)
		return 
	}

	for e = l.Front(); e != nil; e = e.Next()  {
		str:= e.Value.(string)

		if TAOSstrCmp(str,s) {
			continue
		}else {
			l.InsertBefore(s,e)
			return
		}
	}
}

func TAOSSerializeMetrics(m []metric) (sqlcmd []byte, idx int, err error){
	return nil,0,nil
}


func ProcessReq(req Metrics) error{

	tsmap := make(map[int64]map[string][]metric)
	tslist := list.New()
	addr := req.HostIP

	var lastTs int64 = 0
	 for i:=0; i<len(req.Metrics); i++{
		m := req.Metrics[i]
		if tsmap[m.TimeStamp]== nil {
			tsmap[m.TimeStamp] = make(map[string][]metric)
		}

		mp := tsmap[m.TimeStamp]
		mp[m.Name] = append(mp[m.Name],m)

		if lastTs != m.TimeStamp { //there is still some case that will make mistake, when the timestamp is totally out of order. but right now just forget it.
			OrderInsert(m.TimeStamp,tslist)	
		}
		lastTs = m.TimeStamp
	 }
	 
	 for e := tslist.Front();e!=nil; e = e.Next() {
		 
		 namemap,ok := tsmap[e.Value.(int64)]
		 if ok {
			 for k,v := range namemap {

				TAOSCreateStable(v,dbname,addr)
				sqlCmd,idx,_:= TAOSSerializeMetrics(v)

				if sqlCmd != nil{
					batchChans[idx%sqlworkers]<- string(sqlCmd)
				}else {
					continue
					fmt.Println(k)
					//info := fmt.Sprintf("serilize faild, stbname %s",k)
					//panic(info)
				}
			}
		 }else {
			 info := fmt.Sprintf("ProcessReq: cannot retrieve map")
			 panic(info)
		 }

	 }

	return nil
}

func CheckStable(ts []metric,hostip string) (sql string,err error){

	nt,ok := nametagmap[ts[0].Name]

	if !ok {
		nt.taglist = list.New()
		nt.tagmap = make(map[string]string)
		nametagmap[ts[0].Name] = nt
		tagmap :=nt.tagmap
		taglist := nt.taglist
		for i:=0;i<len(ts);i++{
			for k,_ := range ts[i].Tags {
				_,ok := tagmap[k]
				if !ok {
					taglist.PushBack(k)
					tagmap[k] = "y"
				}			
			}
		}
		var sqlcmd string
		sqlcmd = "create table if not exists "+ts[0].Name+" (ts timestamp, value double) tags("
		for e:= taglist.Front(); e!=nil; e = e.Next() {
			sqlcmd = sqlcmd + e.Value.(string) + " binary(40),"
		}
		sqlcmd = sqlcmd +"srcip binary(20), field binary(40))\n"
		fmt.Print(sqlcmd)
		return sqlcmd, nil
	}

	tagmap :=nt.tagmap
	taglist := nt.taglist

	var sqlcmd string 
	for i:=0;i<len(ts);i++{
		
		for k,_ := range ts[i].Tags {
			_,ok := tagmap[k]
			if !ok {
				sqlcmd =sqlcmd+ "alter table "+ts[0].Name+ " add tag " + k + " binary(40)\n" 
				taglist.PushBack(k)
				tagmap[k] = "y"

			}			
		}

	}
	fmt.Print(sqlcmd)
	return sqlcmd,nil
}

func TAOSCreateStable(ts []metric,dbn string, hostip string) error {
	sqlcmd,err := CheckStable(ts,hostip)
	if len(sqlcmd) != 0{
		execSql(dbn,sqlcmd)
	}
	return err
	

//	var schema tdschema
//	var vmp = make(map[string]bool)
//var tbn string
/*
	if len(ts)>0 {
		schema.StbName = ts[0].Name
		schema.Tags = list.New()
		schema.Values = list.New()
		
		for k,_ := range ts[0].Tags {
			OrderInsertS(k,schema.Tags)			
		}
		schema.MultiMetric = false;
		
		for kk,_ := range ts[0].Fields {
			vmp[kk] = true
			OrderInsertS(kk,schema.Values)
		}		
	}
*/	
	

		//fmt.Print(ts[0].Name)
		//fmt.Print(" tags: ")
		//for e := taglist.Front(); e != nil; e = e.Next() {
		//	fmt.Print(e.Value.(string))
	//		fmt.Print(", ")
	//	}
/*

		tbn = ts[i].Name
		
		for e := taglist.Front(); e != nil; e = e.Next() {
			//tagv,_ := ts[i].Tags[e.Value.(string)]
			tbn = tbn+":"+tagv
		}

		for k,_ := range ts[i].Fields {
			s := tbn +":"+ k //+ "_"+hostip
			stbname := ts[i].Name+":"+k
			_,ok := stbmap[s]
			if ok {
				info := fmt.Sprint("same stable name : %s",s)
				panic(info)
			}
			stbmap[s] = true
				
			fmt.Print(stbname)
			fmt.Print("-->")
			fmt.Print(s)
			fmt.Print("\n")
		}

		_,ok := tagmap()
		stbn := ts[i].Name + "_"+strconv.Itoa(len(ts[i].Tags))+"_"+strconv.Itoa(len(ts[i].Fields))
		fmt.Println(stbn)
		if ts[i].Name == "mem" ||ts[i].Name == "system"  {
			fmt.Println(ts[i].Tags)
			fmt.Println(ts[i].Fields)
		}	

		for k,_ := range ts[i].Fields {
			_,ok:=vmp[k]
			if !ok {
				vmp[k] = true
				OrderInsertS(k,schema.Values)
				schema.MultiMetric = true
			}else {
				continue
			}
			
		}
		if schema.MultiMetric == false {
			break;
		}

	}
	
	fmt.Print(" name: ")
	fmt.Println(schema.StbName)
	for e := schema.Values.Front(); e!=nil; e = e.Next() {
		fmt.Print(e.Value)
		fmt.Print(", ")
	}
	fmt.Print("\n")
	fmt.Println("********")
	for e := schema.Tags.Front(); e!=nil; e = e.Next() {
		fmt.Print(e.Value)
		fmt.Print(", ")
	}

*/	


/*
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbn)
	if err != nil {
		log.Fatalf("TAOSCreateStable Open database error: %s\n", err)
		return err
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
	*/
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

func execSql(dbname string,sqlcmd string) {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonUrl+")/"+dbname)
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
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