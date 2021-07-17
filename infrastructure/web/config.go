package web

import (
	"github.com/gin-contrib/cors"
	"os"
	"strconv"
	"strings"
)

type CorsConfig struct {
	AllowAllOrigins  bool
	AllowOrigins     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	AllowCredentials bool
	AllowWebSockets  bool
}

func (conf *CorsConfig) Init() {
	if !conf.AllowAllOrigins {
		if val := os.Getenv("CORS_ALLOW_ALL_ORIGINS"); val != "" {
			conf.AllowAllOrigins, _ = strconv.ParseBool(val)
		}
	}

	if len(conf.AllowOrigins) == 0 {
		if val := os.Getenv("CORS_ALLOW_ORIGINS"); val != "" {
			conf.AllowOrigins = strings.Split(val, ",")
		}
	}

	allowHeaders := []string{"Authorization", "AccessKey", "X-AccessKey", "UserToken", "X-Language", "X-WebAppID", "X-App", "X-Action"}
	if len(conf.AllowHeaders) == 0 {
		if val := os.Getenv("CORS_ALLOW_HEADERS"); val != "" {
			allowHeaders = append(allowHeaders, strings.Split(val, ",")...)
		}
	}
	conf.AllowHeaders = allowHeaders

	if !conf.AllowCredentials {
		if val := os.Getenv("CORS_ALLOW_CREDENTIALS"); val != "" {
			conf.AllowCredentials, _ = strconv.ParseBool(val)
		} else {
			conf.AllowCredentials = true
		}
	}

	if !conf.AllowWebSockets {
		if val := os.Getenv("CORS_ALLOW_WEBSOCKETS"); val != "" {
			conf.AllowWebSockets, _ = strconv.ParseBool(val)
		} else {
			conf.AllowWebSockets = true
		}
	}
}

func (conf *CorsConfig) GetConfig() cors.Config {
	corsConfig := cors.DefaultConfig()
	if conf.AllowAllOrigins {
		corsConfig.AllowAllOrigins = true
	} else {
		if len(conf.AllowOrigins) == 0 {
			corsConfig.AllowOrigins = []string{
				"http://127.0.0.1",
			}
		} else {
			corsConfig.AllowOrigins = conf.AllowOrigins
		}
	}
	if len(conf.AllowHeaders) > 0 {
		corsConfig.AddAllowHeaders(conf.AllowHeaders...)
	}

	corsConfig.AllowCredentials = conf.AllowCredentials
	corsConfig.AllowWebSockets = conf.AllowWebSockets
	corsConfig.AllowWildcard = true
	corsConfig.ExposeHeaders = []string{"Authorization"}
	return corsConfig
}
