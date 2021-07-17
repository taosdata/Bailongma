package web

import "github.com/gin-gonic/gin"

type ServiceController interface {
	Init(router gin.IRouter)
}

var Controllers []ServiceController

func AddController(controller ServiceController) {
	Controllers = append(Controllers, controller)
}
