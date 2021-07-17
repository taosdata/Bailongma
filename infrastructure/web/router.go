package web

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"net/http"
)

func CreateRouter(debug bool, corsConf *CorsConfig, enableGzip bool) *gin.Engine {
	if debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.Default()

	if debug {
		pprof.Register(router)
	}
	if enableGzip {
		router.Use(gzip.Gzip(gzip.DefaultCompression))
	}
	router.Use(cors.New(corsConf.GetConfig()))
	return router
}

type BaseController struct {
}

func (*BaseController) SuccessResponse(c *gin.Context, msg interface{}) {
	c.JSON(http.StatusOK, gin.H{"success": true, "msg": msg})
}

func (*BaseController) FailResponse(c *gin.Context, msg string) {
	c.JSON(http.StatusOK, gin.H{"success": false, "msg": msg})
}

func (*BaseController) BadRequestResponse(c *gin.Context, msg string) {
	c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"msg": msg})
}

func (*BaseController) InternalErrorResponse(c *gin.Context, err error) {
	c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
}

func (*BaseController) InternalErrorGroupResponse(c *gin.Context, errs []error) {
	c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"msg": errs})
}
