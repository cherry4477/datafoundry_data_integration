package handler

import (
	"github.com/asiainfoLDP/datafoundry_data_integration/api"
	"github.com/asiainfoLDP/datafoundry_data_integration/common"
	"github.com/asiainfoLDP/datafoundry_data_integration/models"
	"github.com/julienschmidt/httprouter"
	"math/rand"
	"net/http"
	"time"
)

const (
	TransTypeDEDUCTION = "deduction"
	TransTypeRECHARGE = "recharge"

	letterBytes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var AdminUsers = []string{"admin", "datafoundry"}

type Aipayrecharge struct {
	Order_id  string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	ReturnUrl string  `json:"returnUrl"`
}

type Result struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg,omitempty"`
	Data interface{} `json:"data,omitempty"`
}

type AipayRequestInfo struct {
	Aiurl    string        `json:"aiurl"`
	Method   string        `json:"method"`
	Payloads []PayloadInfo `json:"payloads"`
}

type PayloadInfo struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type NotifyResult struct {
	SignPayNotifyMsg string `json:"signPayNotifyMsg"`
	Order_id         string `json:"order_id"`
	Result           int    `json:"result"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func CreateRepoHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Info("Request url: POST %v.", r.URL)

	logger.Info("Begin do CreateRepo handler.")
	defer logger.Info("End do recharge handler.")

	db := models.GetDB()
	if db == nil {
		logger.Warn("Get db is nil.")
		api.JsonResult(w, http.StatusInternalServerError, api.GetError(api.ErrorCodeDbNotInitlized), nil)
		return
	}

	repo := &models.Repository{}
	err := common.ParseRequestJsonInto(r, repo)
	if err != nil {
		logger.Error("Parse body err: %v", err)
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeParseJsonFailed, err.Error()), nil)
		return
	}

	repo.Status = "A"

	err = models.RecordRepo(db, repo)
	if err != nil {
		logger.Error("Record repository err: %v", err)
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeRecordRepository, err.Error()), nil)
		return
	}

	api.JsonResult(w, http.StatusOK, nil, nil)
}

func QueryRepoListHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Info("Request url: GET %v.", r.URL)

	logger.Info("Begin get RepoList handler.")
	defer logger.Info("End get RepoList handler.")

	db := models.GetDB()
	if db == nil {
		logger.Warn("Get db is nil.")
		api.JsonResult(w, http.StatusInternalServerError, api.GetError(api.ErrorCodeDbNotInitlized), nil)
		return
	}

	r.ParseForm()

	offset, size := api.OptionalOffsetAndSize(r, 30, 1, 1000)
	sortOrder := models.ValidateSortOrder(r.Form.Get("sortorder"), models.SortOrderDesc)
	orderBy := models.ValidateOrderBy(r.Form.Get("orderby"))
	class := r.Form.Get("class")
	label := r.Form.Get("label")
	reponame := r.Form.Get("reponame")

	count, repos, err := models.QueryRepoList(db, class, label, reponame, orderBy, sortOrder, offset, size)
	if err != nil {
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeQueryRepositorys, err.Error()), nil)
		return
	}
	api.JsonResult(w, http.StatusOK, nil, api.NewQueryListResult(count, repos))

}

func QueryRepoHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Info("Request url: GET %v.", r.URL)

	logger.Info("Begin get Repo handler.")
	defer logger.Info("End get Repo handler.")

	db := models.GetDB()
	if db == nil {
		logger.Warn("Get db is nil.")
		api.JsonResult(w, http.StatusInternalServerError, api.GetError(api.ErrorCodeDbNotInitlized), nil)
		return
	}

	repoName := params.ByName("reponame")
	if repoName == "" {
		logger.Warn("repoName is nil")
		api.JsonResult(w, http.StatusBadRequest, api.GetError(api.ErrorCodeNone), nil)
	}

	repo, err := models.QueryRepo(db, repoName)
	if err != nil {
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeQueryRepositorys, err.Error()), nil)
		return
	}
	items, err := models.QueryItemList(db, repoName)
	if err != nil {
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeQueryDataitemss, err.Error()), nil)
		return
	}
	result := struct {
		*models.Repository
		Items []*models.Dataitem  `json:"items"`
	}{
		repo,
		items,
	}
	api.JsonResult(w, http.StatusOK, nil, result)
}

func QueryDataItemHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Info("Request url: GET %v.", r.URL)

	logger.Info("Begin get DataItem handler.")
	defer logger.Info("End get DataItem handler.")

	db := models.GetDB()
	if db == nil {
		logger.Warn("Get db is nil.")
		api.JsonResult(w, http.StatusInternalServerError, api.GetError(api.ErrorCodeDbNotInitlized), nil)
		return
	}

	itemName := params.ByName("itemname")
	repoName := params.ByName("reponame")

	item, err := models.QueryItem(db, repoName, itemName)
	if err != nil {
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeQueryDataitemss, err.Error()), nil)
		return
	}
	itemId := item.ItemId
	attrs, err := models.QueryAttrList(db, itemId)

	if err != nil {
		api.JsonResult(w, http.StatusBadRequest, api.GetError2(api.ErrorCodeQueryAttribute, err.Error()), nil)
		return
	}
	repo,err := models.QueryRepo(db,repoName)
	var res struct {
		*models.Dataitem
		CreateUser string	   `json:"createUser"`
		Attrs []*models.Attribute  `json:"attrs"`
	}
	res.CreateUser = repo.CreateUser
	res.Dataitem = item
	res.Attrs = attrs
	api.JsonResult(w, http.StatusOK, nil, res)
}

func CheckAmount(amount float64) uint {

	amount = float64(int64(amount * 100)) * 0.01
	/*if (amount*100 - float64(int64(amount*100))) > 0 {
		logger.Error("%v, %v, %v, %v", api.ErrorCodeAmountsInvalid, amount, amount*100, float64(int(amount*100)))
		return api.ErrorCodeAmountsInvalid
	}*/
	logger.Info("amount:%v", amount)
	if amount < 0 {
		logger.Error("%v, %v", api.ErrorCodeAmountsNegative, amount)
		return api.ErrorCodeAmountsNegative
	}
	if amount > 99999999.99 {
		logger.Error("%v, %v", api.ErrorCodeAmountsTooBig, amount)
		return api.ErrorCodeAmountsTooBig
	}

	return api.ErrorCodeNone
}
