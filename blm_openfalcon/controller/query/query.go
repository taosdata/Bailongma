package query

import (
	"bailongma/v2/blm_openfalcon/metricgroup"
	"bailongma/v2/blm_openfalcon/model"
	"bailongma/v2/blm_openfalcon/pool"
	"bailongma/v2/blm_openfalcon/tdengine"
	"bailongma/v2/infrastructure/util"
	"bailongma/v2/infrastructure/web"
	"errors"
	"github.com/gin-gonic/gin"
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	"github.com/open-falcon/falcon-plus/modules/api/app/helper"
	"net/http"
	"strings"
	"time"
)

type Query struct {
	web.BaseController
}

func (ctl *Query) Init(router gin.IRouter) {
	api := router.Group("/api/v1")
	api.POST("/graph/history", ctl.history)
	api.POST("/graphQuery", ctl.graphQuery)
}

func (ctl *Query) history(c *gin.Context) {
	req := model.HistoryRequest{}
	err := c.BindJSON(&req)
	if err != nil {
		ctl.errorResponse(c, http.StatusBadRequest, err)
		return
	}
	var singleMetricList []string
	groupMetricMap := map[string][]string{} //metricGroup : [metric]
	for _, counter := range req.Counters {
		metric, _ := parseCounter(counter)
		groupInfo, isGroup := metricgroup.GetMetricGroupInfo(metric)
		if !isGroup {
			singleMetricList = append(singleMetricList, counter)
		} else {
			groupMetricMap[groupInfo.Group] = append(groupMetricMap[groupInfo.Group], counter)
		}
	}
	start := time.Unix(req.StartTime, 0)
	end := time.Unix(req.EndTime, 0)
	interval := req.Step
	if interval == 0 {
		interval = 30
	}
	aggregation := ""
	switch req.ConsolFun {
	case "AVERAGE":
		aggregation = "avg"
	case "MAX":
		aggregation = "max"
	case "MIN":
		aggregation = "min"
	case "Last":
		aggregation = "last"
	}
	if aggregation == "" {
		ctl.errorResponse(c, http.StatusBadRequest, errors.New("unsupported aggregation"))
		return
	}
	fill := "LINEAR"
	queryData, metricHashMap, err := tdengine.QueryMetric(req.HostNames, singleMetricList, groupMetricMap, start, end, interval, aggregation, fill)
	if err != nil {
		ctl.errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	data := tdengine.OrganizeResult2HistoryResponse(queryData, metricHashMap, interval, "")
	ctl.successResponse(c, data)
	for _, response := range *data {
		for _, value := range response.Values {
			pool.RRDPoolPut(value)
		}
	}
}

func (ctl *Query) errorResponse(c *gin.Context, code int, err error) {
	body := helper.RespJson{Error: err.Error()}
	c.JSON(code, body)
}

func (ctl *Query) successResponse(c *gin.Context, msg interface{}) {
	switch msg.(type) {
	case string:
		body := helper.RespJson{Msg: msg.(string)}
		c.JSON(http.StatusOK, body)
	default:
		c.JSON(http.StatusOK, msg)
	}
}

func (ctl *Query) graphQuery(c *gin.Context) {
	var req *cmodel.GraphQueryParam
	err := c.BindJSON(&req)
	if err != nil {
		ctl.BadRequestResponse(c, err.Error())
		return
	}
	metric, tag := parseCounter(req.Counter)
	column := "value"
	groupInfo, isGroup := metricgroup.GetMetricGroupInfo(metric)
	if isGroup {
		column = util.ToHashString(metric)
		metric = groupInfo.Group
	}
	start := time.Unix(req.Start, 0)
	end := time.Unix(req.End, 0)
	interval := req.Step
	if interval == 0 {
		interval = 30
	}
	aggregation := ""
	switch req.ConsolFun {
	case "AVERAGE":
		aggregation = "avg"
	case "MAX":
		aggregation = "max"
	case "MIN":
		aggregation = "min"
	case "Last":
		aggregation = "last"
	}
	if aggregation == "" {
		ctl.errorResponse(c, http.StatusBadRequest, errors.New("unsupported aggregation"))
		return
	}
	fill := "LINEAR"
	b := pool.BytesPoolGet()
	b.WriteString(req.Endpoint)
	b.WriteByte('/')
	b.WriteString(metric)
	b.WriteByte('/')
	b.WriteString(tag)
	tableName := util.ToHashString(b.String())
	pool.BytesPoolPut(b)
	queryData, err := tdengine.QueryMetricDirectly(tableName, column, start, end, interval, aggregation, fill)
	if err != nil {
		ctl.errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	ctl.successResponse(c, queryData)
	for _, d := range queryData {
		pool.FloatValueResponsePoolPut(d)
	}
}

func parseCounter(counter string) (metric string, tag string) {
	s := strings.SplitN(counter, "/", 2)
	if len(s) == 2 {
		return s[0], s[1]
	} else {
		return s[0], ""
	}
}

func init() {
	collector := &Query{
		BaseController: web.BaseController{},
	}
	web.AddController(collector)
}
