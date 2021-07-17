package main

import (
	"bailongma/v2/blm_openfalcon/config"

	_ "bailongma/v2/blm_openfalcon/controller"
	"bailongma/v2/infrastructure/web"
	"fmt"
	"golang.org/x/sync/errgroup"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	var g errgroup.Group
	g.Go(runWebServer)
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-quit
	fmt.Println("stop server")
}

func runWebServer() error {
	conf := config.Conf
	fmt.Printf("start web on :%d\n", conf.Port)
	router := web.CreateRouter(conf.Debug, &conf.Cors, conf.EnableGzip)
	for _, controller := range web.Controllers {
		controller.Init(router)
	}
	server := &http.Server{
		Addr:              ":" + strconv.Itoa(conf.Port),
		Handler:           router,
		ReadHeaderTimeout: 20 * time.Second,
		ReadTimeout:       200 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	return server.ListenAndServe()
}
