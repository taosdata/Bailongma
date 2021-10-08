package remotewrite

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
	api := router.Group("write")
	api.POST("", ctl.write)
}

func (ctl *Controller) write(c *gin.Context) {
	c.Status(http.StatusAccepted)
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
	var req prompb.WriteRequest
	err = proto.Unmarshal(buf, &req)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	_ = processReq(&req)
}
func init() {
	collector := &Controller{
		BaseController: web.BaseController{},
	}
	web.AddController(collector)
}
