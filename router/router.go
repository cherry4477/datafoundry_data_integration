package router

import (
	"github.com/asiainfoLDP/datafoundry_data_integration/api"
	"github.com/asiainfoLDP/datafoundry_data_integration/handler"
	"github.com/asiainfoLDP/datafoundry_data_integration/log"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"time"
)

const (
	Platform_Local  = "local"
	Platform_DataOS = "dataos"
)

var (
	Platform = Platform_DataOS
	logger   = log.GetLogger()
)

//==============================================================
//
//==============================================================

func handler_Index(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	api.JsonResult(w, http.StatusNotFound, api.GetError(api.ErrorCodeUrlNotSupported), nil)
}

func httpNotFound(w http.ResponseWriter, r *http.Request) {
	api.JsonResult(w, http.StatusNotFound, api.GetError(api.ErrorCodeUrlNotSupported), nil)
}

type HttpHandler struct {
	handler http.HandlerFunc
}

func (httpHandler *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if httpHandler.handler != nil {
		httpHandler.handler(w, r)
	}
}

//==============================================================
//
//==============================================================

func InitRouter() *httprouter.Router {
	router := httprouter.New()
	router.RedirectFixedPath = false
	router.RedirectTrailingSlash = false

	router.POST("/", handler_Index)
	router.DELETE("/", handler_Index)
	router.PUT("/", handler_Index)
	router.GET("/", handler_Index)

	router.NotFound = &HttpHandler{httpNotFound}
	router.MethodNotAllowed = &HttpHandler{httpNotFound}

	return router
}

func NewRouter(router *httprouter.Router) {
	logger.Info("new router.")
	router.POST("/integration/v1/repository", api.TimeoutHandle(35000*time.Millisecond, handler.CreateRepoHandler))
	router.GET("/integration/v1/repositories", api.TimeoutHandle(35000*time.Millisecond, handler.QueryRepoListHandler))
	router.GET("/integration/v1/repository/:reponame", api.TimeoutHandle(35000*time.Millisecond, handler.QueryRepoHandler))
	router.GET("/integration/v1/dataitem/:reponame/:itemname", api.TimeoutHandle(35000*time.Millisecond, handler.QueryDataItemHandler))

	//router.GET("/saasappapi/v1/apps", api.TimeoutHandle(500*time.Millisecond, QueryAppList))
}
