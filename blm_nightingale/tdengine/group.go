package tdengine

import (
	"context"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/tdengine/connector"
	"strings"
	"sync"
	"time"
)

func GetAllSTables() map[string]int {
	wg := sync.WaitGroup{}
	ctx := context.Background()
	var result = map[string]int{}
	lock := sync.RWMutex{}
	for index, executor := range ExecutorGroup {
		wg.Add(1)
		i := index
		e := executor
		poolErr := pool.GoroutinePool.Submit(func() {
			defer wg.Done()
			d, err := e.executor.GetAllStableNames(ctx)
			if err != nil {
				return
			}
			lock.Lock()
			for _, metric := range d {
				result[metric] = i
			}
			lock.Unlock()
		})
		if poolErr != nil {
			wg.Done()
		}
	}
	wg.Wait()
	return result
}

func GetBySTables(start time.Time, end time.Time, sTables map[string]int, whereCondition []string) map[string]*connector.Data {
	var result = map[string]*connector.Data{}
	lock := sync.RWMutex{}
	sTableMap := map[int][]string{}
	for s, i := range sTables {
		sTableMap[i] = append(sTableMap[i], s)
	}
	wg := sync.WaitGroup{}
	for index, subSTables := range sTableMap {
		for _, sTable := range subSTables {
			db := ExecutorGroup[index]
			wg.Add(1)
			sTableName := sTable
			poolErr := pool.GoroutinePool.Submit(func() {
				defer wg.Done()
				b := pool.BytesPoolGet()
				b.WriteString("select * from ")
				b.WriteString(db.executor.WithDBName(sTableName))
				b.WriteString(" where ts >='")
				b.WriteString(formatTime(start))
				b.WriteString("' and ts <='")
				b.WriteString(formatTime(end))
				b.WriteByte('\'')
				if len(whereCondition) > 0 {
					b.WriteString(" and ")
					b.WriteString(strings.Join(whereCondition, " and "))
				}
				sql := b.String()
				pool.BytesPoolPut(b)
				data, err := db.executor.DoQuery(db.ctx, sql)
				if err == nil {
					lock.Lock()
					result[sTableName] = data
					lock.Unlock()
				}
			})
			if poolErr != nil {
				wg.Done()
			}
		}
	}
	wg.Wait()
	return result
}