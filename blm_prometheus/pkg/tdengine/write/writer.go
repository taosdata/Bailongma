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
	"bailongma/v2/blm_prometheus/pkg/log"
	"container/list"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type WriterProcessor struct {
}

type nameTag struct {
	tagMap  map[string]string
	tagList *list.List
}

func NewProcessor() *WriterProcessor {
	processor := &WriterProcessor{}
	return processor
}

func (p *WriterProcessor) Process(req *prompb.WriteRequest) error {
	db, err := sql.Open(DriverName, DbUser+":"+DbPassword+"@/tcp("+DaemonIP+")/"+DbName)
	if err != nil {
		log.ErrorLogger.Printf("Open database error: %s\n", err)
	}
	defer db.Close()

	for _, ts := range req.Timeseries {
		err = HandleStable(ts, db)
	}
	return err
}

func (p *WriterProcessor) Check(r *http.Request) (string, error) {
	var output string = ""
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
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
	return output, nil
}

func HandleStable(ts *prompb.TimeSeries, db *sql.DB) error {
	tagList := list.New()
	tbTagList := list.New()
	tagMap := make(map[string]string)
	tbTagMap := make(map[string]string)
	m := make(model.Metric, len(ts.Labels))
	tagNum := TagNumLimit
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
			if len(s) > TagLen {
				s = s[:TagLen]
			}
			tbTagMap[ln] = "y"
		}
		tagMap[ln] = s
	}

	if DebugPrt == 2 {
		t := ts.Samples[0].Timestamp
		var ns int64 = 0
		if t/1000000000 > 10 {
			tm := t / 1000
			ns = t - tm*1000
		}
		log.DebugLogger.Printf(" Ts: %s, value: %f, ", time.Unix(t/1000, ns), ts.Samples[0].Value)
		log.DebugLogger.Println(ts)
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
				taosTagList := list.New()
				taosTagMap := make(map[string]string)
				dt := stbSt["data"]
				for _, fd := range dt.([]interface{}) {
					fdc := fd.([]interface{})
					if fdc[3].(string) == "tag" && fdc[0].(string) != "taghash" {
						tmpStr := fdc[0].(string)
						taosTagList.PushBack(tmpStr[2:])
						taosTagMap[tmpStr[2:]] = "y"
					}
				}
				nt.tagList = taosTagList
				nt.tagMap = taosTagMap
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
					if i < TagNumLimit {
						_, ok := tbTagMap[k]
						if !ok {
							sqlcmd = "alter table " + sTableName + " add tag t_" + k + TagStr + "\n"
							_, err := execSql(sqlcmd, db)
							if err != nil {
								errorCode := fmt.Sprintf("%s", err)
								if strings.Contains(errorCode, "duplicated column names") {
									tbTagList.PushBack(k)
									//OrderInsertS(k, tbTagList)
									tbTagMap[k] = "y"
								} else {
									log.ErrorLogger.Println(err)
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
					sqlcmd = sqlcmd + ",t_" + e.Value.(string) + TagStr
				}

				sqlcmd = sqlcmd + ")\n"
				_, err := execSql(sqlcmd, db)
				if err == nil {
					IsSTableCreated.Store(sTableName, nt)
				} else {
					log.ErrorLogger.Println(err)
				}
			}
		} else { //query TDengine table error
			log.ErrorLogger.Println(err)
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
			if i < TagNumLimit {
				_, ok := tbTagMap[k]
				if !ok {
					sqlcmd = "alter table " + sTableName + " add tag t_" + k + TagStr + "\n"
					_, err := execSql(sqlcmd, db)
					if err != nil {
						log.ErrorLogger.Println(err)
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

	tableNameHash := "MD5_" + md5V2(tbn)
	_, tbcreated := IsTableCreated.Load(tableNameHash)

	if !tbcreated {
		var sqlcmdhead, sqlcmd string
		sqlcmdhead = "create table if not exists " + tableNameHash + " using " + sTableName + " tags(\""
		sqlcmd = ""
		i := 0
		for e := tbTagList.Front(); e != nil; e = e.Next() {
			tagValue, has := tagMap[e.Value.(string)]
			if len(tagValue) > TagLen {
				tagValue = tagValue[:TagLen]
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
		_, err := execSql(sqlcmd, db)
		if err == nil {
			IsTableCreated.Store(tableNameHash, true)
		}
	}
	serializeTDengine(ts, tableNameHash, db)
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

func queryTableStruct(tbname string) string {
	client := new(http.Client)
	s := fmt.Sprintf("describe %s.%s", DbName, tbname)
	body := strings.NewReader(s)
	req, _ := http.NewRequest("GET", "http://"+TdUrl+":"+ApiPort+"/rest/sql", body)
	//fmt.Println("http://" + tdurl + ":" + apiPort + "/rest/sql" + s)
	req.SetBasicAuth(DbUser, DbPassword)
	resp, err := client.Do(req)

	if err != nil {
		log.ErrorLogger.Println(err)
		fmt.Println(err)
		return ""
	} else {
		compressed, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return string(compressed)
	}
}

func execSql(sqlcmd string, db *sql.DB) (sql.Result, error) {
	if len(sqlcmd) < 1 {
		return nil, nil
	}
	if DebugPrt == 2 {
		log.DebugLogger.Println(sqlcmd)
	}
	res, err := db.Exec(sqlcmd)
	if err != nil {
		log.InfoLogger.Printf("execSql Error: %s sqlcmd: %s\n", err, sqlcmd)

	}
	return res, err
}

func md5V2(str string) string {
	data := []byte(str)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	return md5str
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
	BatchChannels[idx%SqlWorkers] <- sqlcmd
	return nil
}

func TAOSHashID(ba []byte) int {
	var sum int = 0
	for i := 0; i < len(ba); i++ {
		sum += int(ba[i] - '0')
	}
	return sum
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
