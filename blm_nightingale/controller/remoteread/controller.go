package remoteread

import (
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/go-utils/web"
	"net/http"
)

type Controller struct {
	web.BaseController
}

func (ctl *Controller) Init(router gin.IRouter) {
	api := router.Group("read")
	api.POST("", ctl.read)
}

func (ctl *Controller) read(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	buf, err := snappy.Decode(nil, data)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	var req prompb.ReadRequest
	err = proto.Unmarshal(buf, &req)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	resp, err := process(&req)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	respData, err := proto.Marshal(resp)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	compressed := snappy.Encode(nil, respData)
	c.Header("Content-Encoding", "snappy")
	c.Data(http.StatusAccepted, "application/x-protobuf", compressed)
}
func init() {
	collector := &Controller{
		BaseController: web.BaseController{},
	}
	web.AddController(collector)
}
