package collector

import (
	"github.com/gin-gonic/gin"
	"github.com/taosdata/Bailongma/blm_openfalcon/log"
	"github.com/taosdata/Bailongma/blm_openfalcon/metricgroup"
	"github.com/taosdata/Bailongma/blm_openfalcon/model"
	"github.com/taosdata/Bailongma/blm_openfalcon/tdengine"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/web"
)

type Collector struct {
	web.BaseController
}

func (ctl *Collector) Init(router gin.IRouter) {
	api := router.Group("/openfalcon")
	api.POST("", ctl.receive)
}

func (ctl *Collector) receive(c *gin.Context) {
	var req []*model.MetricValue
	err := c.ShouldBindJSON(&req)
	if err != nil {
		log.Logger.WithError(err).Error("parse metric data error")
		ctl.FailResponse(c, err.Error())
		return
	}
	metricGroupMap := map[string][]*model.MetricValue{}
	metricPatternGroupMap := map[string][]*model.MetricValue{}
	metricSingleMap := map[string][]*model.MetricValue{}
	for _, metric := range req {
		metricGroup, isPreDefined := metricgroup.GetMetricGroupInfo(metric.Metric)
		if isPreDefined {
			//多列
			if metricGroup.PatternMatched {
				//正则匹配到
				metricPatternGroupMap[metricGroup.Group] = append(metricPatternGroupMap[metricGroup.Group], metric)
			} else {
				//未匹配到正则
				metricGroupMap[metricGroup.Group] = append(metricGroupMap[metricGroup.Group], metric)
			}
		} else {
			//单列
			b := pool.BytesPoolGet()
			b.WriteString(metric.Endpoint)
			b.WriteByte('/')
			b.WriteString(metric.Metric)
			b.WriteByte('/')
			b.WriteString(metric.Tags)
			tableName := b.String()
			pool.BytesPoolPut(b)
			metricSingleMap[tableName] = append(metricSingleMap[tableName], metric)
		}
	}
	//修正正则超级表
	for sTableName, metrics := range metricPatternGroupMap {
		err = tdengine.ModifyTable(sTableName, metrics)
		if err != nil {
			//修正表失败不继续插入数据
			delete(metricPatternGroupMap, sTableName)
		}
	}
	var haveErr bool
	var errGroup [3]error
	//组数据入库
	err = tdengine.InsertCommonGroupMetric(metricGroupMap)
	if err != nil {
		haveErr = true
		errGroup[0] = err
	}
	//单列数据入库
	err = tdengine.InsertSingleColumnMetric(metricSingleMap)
	if err != nil {
		haveErr = true
		errGroup[1] = err
	}
	//正则数据入库
	err = tdengine.InsertPatternGroupMetric(metricPatternGroupMap)
	if err != nil {
		haveErr = true
		errGroup[2] = err
	}
	if haveErr {
		ctl.InternalErrorGroupResponse(c, errGroup[:])
		return
	}
	ctl.SuccessResponse(c, nil)
}

func init() {
	collector := &Collector{
		BaseController: web.BaseController{},
	}
	web.AddController(collector)
}
