package models

import (
	"database/sql"
	"fmt"
	"strings"

	"time"
)

const (
	SortOrderDesc = "desc"
	SortOrderAsc  = "asc"
)

type Transaction struct {
	TransactionId string    `json:"transactionId"`
	Type          string    `json:"type"`
	Amount        float64   `json:"amount"`
	Namespace     string    `json:"namespace"`
	User          string    `json:"user,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	Region        string    `json:"region,omitempty"`
	Paymode       string    `json:"paymode,omitempty"`
	CreateTime    time.Time `json:"createtime,omitempty"`
	Status        string    `json:"status,omitempty"`
	StatusTime    time.Time `json:"statustime,omitempty"`
}
type Repository struct {
	RepoId        int	`json:"repoId,omitempty"`
	RepoName      string	`json:"repoName,omitempty"`
	Class	      string	`json:"class,omitempty"`
	Label         string	`json:"label,omitempty"`
	CreateUser    string	`json:"createUser,omitempty"`
	Description   string	`json:"description,omitempty"`
	CreateTime    *time.Time	`json:"createTime,omitempty"`
	UpdateTime    *time.Time	`json:"updateTime,omitempty"`
	Status	      string	`json:"status,omitempty"`
}

type Dataitem struct {
	ItemId			int		`json:"itemId,omitempty"`
	ItemName     		string		`json:"itemName,omitempty"`
	RepoName      		string		`json:"repoName,omitempty"`
	Url	      		string		`json:"url,omitempty"`
	CreateName	      	*time.Time	`json:"createName,omitempty"`
	UpdateName	      	*time.Time	`json:"updateName,omitempty"`
	Status	      		string		`json:"status,omitempty"`
	Simple			string		`json:"simple,omitempty"`
}

type Attribute struct {
	ATTR_ID		int		`json:"attrId,omitempty"`
	ITEM_ID		int		`json:"itemId,omitempty"`
	ATTR_NAME	string		`json:"attrName,omitempty"`
	INSTRUCTION	string		`json:"instruction,omitempty"`
	ORDER_ID	int		`json:"orderId,omitempty"`
}

func RecordRecharge(db *sql.DB, rechargeInfo *Transaction) error {
	logger.Info("Model begin record recharge")
	defer logger.Info("Model end record recharge")

	nowstr := time.Now().Format("2006-01-02 15:04:05.999999")
	sqlstr := fmt.Sprintf(`insert into DF_TRANSACTION (
				TRANSACTION_ID, TYPE, AMOUNT, NAMESPACE, USER, REASON, 
				REGION, PAYMODE, CREATE_TIME, STATUS, STATUS_TIME
				) values (
				?, ?, ?, ?, ?, ?, 
				?, ?, '%s', ?, '%s')`,
		nowstr, nowstr)

	_, err := db.Exec(sqlstr,
		rechargeInfo.TransactionId, rechargeInfo.Type, rechargeInfo.Amount, rechargeInfo.Namespace,
		rechargeInfo.User, rechargeInfo.Reason, rechargeInfo.Region, rechargeInfo.Paymode, rechargeInfo.Status)

	return err
}

func QueryTransactionList(db *sql.DB, transType, namespace, status, region, orderBy, sortOrder string,
	offset int64, limit int) (int64, []*Transaction, error) {

	logger.Debug("QueryTransactions begin")

	sqlParams := make([]interface{}, 0, 3)
	sqlwhere := ""
	if status != "" {
		if sqlwhere == "" {
			sqlwhere = fmt.Sprintf("status in (%s)", status)
		} else {
			sqlwhere = sqlwhere + fmt.Sprintf(" and status in (%s)", status)
		}
		//sqlParams = append(sqlParams, status)
	}

	if transType != "" {
		if sqlwhere == "" {
			sqlwhere = "type=?"
		} else {
			sqlwhere = sqlwhere + " and type=?"
		}
		sqlParams = append(sqlParams, transType)
	}

	if namespace != "" {
		if sqlwhere == "" {
			sqlwhere = "namespace=?"
		} else {
			sqlwhere = sqlwhere + " and namespace=?"
		}
		sqlParams = append(sqlParams, namespace)
	}

	if region != "" {
		if sqlwhere == "" {
			sqlwhere = "region=?"
		} else {
			sqlwhere = sqlwhere + " and region=?"
		}
		sqlParams = append(sqlParams, region)
	}

	sqlorder := ""
	if orderBy != "" {
		sqlorder = fmt.Sprintf(" order by %s %s", orderBy, sortOrder)
	}

	count, err := queryTransactionsCount(db, sqlwhere, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return 0, nil, err
	}

	validateOffsetAndLimit(count, &offset, &limit)

	trans, err := queryTransactions(db,
		sqlwhere, sqlorder,
		limit, offset, sqlParams...)

	return count, trans, err
}

func ValidateSortOrder(sortOrder string, defaultOrder string) string {
	switch strings.ToLower(sortOrder) {
	case SortOrderAsc:
		return SortOrderAsc
	case SortOrderDesc:
		return SortOrderDesc
	}

	return defaultOrder
}

func ValidateOrderBy(orderBy string) string {
	switch orderBy {
	case "createtime":
		return "CREATE_TIME"
	}
	return ""
}

func ValidateTransType(transtype string) string {
	switch transtype {
	case "deduction":
		return "deduction"
	case "recharge":
		return "recharge"
	}

	return ""
}

func ValidateStatus(status string) string {
	switch status {
	case "O":
		return "'O'"
	case "I":
		return "'I'"
	case "ALL":
		return ""
	default:
		return "'O', 'I', 'E'"
	}

}

func queryTransactionsCount(db *sql.DB, sqlwhere string, sqlParams ...interface{}) (int64, error) {

	count := int64(0)

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`select COUNT(*) from DF_TRANSACTION %s `, sqlwhereall)
	logger.Debug(">>>\n"+
		"	%s", sqlstr)
	err := db.QueryRow(sqlstr, sqlParams...).Scan(&count)

	return count, err
}

func queryTransactions(db *sql.DB, sqlwhere, sqlorder string,
	limit int, offset int64, sqlParams ...interface{}) ([]*Transaction, error) {

	logger.Info("Model begin queryTransactions")
	defer logger.Info("Model end queryTransactions")

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`SELECT TRANSACTION_ID, TYPE, 
		AMOUNT, NAMESPACE, USER, REASON, REGION, PAYMODE, CREATE_TIME, STATUS,  STATUS_TIME
		FROM DF_TRANSACTION 
		%s 
		%s 
		LIMIT %d OFFSET %d`,
		sqlwhereall,
		sqlorder,
		limit, offset)

	logger.Info(">>> %v", sqlstr)
	rows, err := db.Query(sqlstr, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	defer rows.Close()

	trans := make([]*Transaction, 0, 32)
	for rows.Next() {
		tran := &Transaction{}
		err := rows.Scan(&tran.TransactionId, &tran.Type, &tran.Amount, &tran.Namespace, &tran.User,
			&tran.Reason, &tran.Region, &tran.Paymode, &tran.CreateTime, &tran.Status, &tran.StatusTime)
		if err != nil {
			return nil, err
		}
		trans = append(trans, tran)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return trans, nil
}

func UpdateRechargeAndBalance(db *sql.DB, transid, status string) (err error) {
	err = UpdateTransaction(db, transid, status)
	if err != nil {
		logger.Error("UpdateTransaction:%v", err)
		return
	}

	trans, err := _getTransactionByTransId(db, transid)
	if err != nil {
		logger.Error("_getTransactionById:%v")
		return
	}

	balance, err := RechargeBalance(db, trans.Namespace, trans.Amount)
	if err != nil {
		logger.Error("RechargeBalance:%v", err)
		return err
	}
	logger.Debug("UpdateRechargeAndBalance---RechargeBalance:%v", balance.Balance)
	return err
}

func _getTransactionByTransId(db *sql.DB, transid string) (*Transaction, error) {
	defer logger.Debug("_getTransactionByTransId end.")

	sqlstr := fmt.Sprintf(`SELECT TRANSACTION_ID, TYPE, AMOUNT, NAMESPACE, USER, REASON, REGION, PAYMODE, CREATE_TIME, STATUS, STATUS_TIME FROM DF_TRANSACTION WHERE TRANSACTION_ID=?`)
	logger.Debug("%s---%s", sqlstr, transid)
	row := db.QueryRow(sqlstr, transid)
	t := &Transaction{}
	err := row.Scan(&t.TransactionId, &t.Type, &t.Amount, &t.Namespace, &t.User, &t.Reason,
		&t.Region, &t.Paymode, &t.CreateTime, &t.Status, &t.StatusTime)
	if err != nil {
		return nil, err
	}
	logger.Debug("transaction:%v", t)
	return t, nil
}

func UpdateTransaction(db *sql.DB, transid, status string) error {
	defer logger.Debug("UpdateTransaction end %s, %s", transid, status)

	nowstr := time.Now().Format("2006-01-02 15:04:05.999999")
	sqlstr := fmt.Sprintf(`UPDATE DF_TRANSACTION SET STATUS=? , STATUS_TIME=? WHERE TRANSACTION_ID=?`)

	logger.Debug("%s---%s---%s---%s", sqlstr, status, nowstr, transid)
	_, err := db.Exec(sqlstr, status, nowstr, transid)
	return err
}

func validateOffsetAndLimit(count int64, offset *int64, limit *int) {
	if *limit < 1 {
		*limit = 1
	}
	if *offset >= count {
		*offset = count - int64(*limit)
	}
	if *offset < 0 {
		*offset = 0
	}
	if *offset+int64(*limit) > count {
		*limit = int(count - *offset)
	}
}

func RecordRepo(db *sql.DB, repositoryInfo *Repository) error {
	logger.Info("Model begin record repository")
	defer logger.Info("Model end record repository")

	nowstr := time.Now().Format("2006-01-02 15:04:05.999999")
	sqlstr := fmt.Sprintf(`insert into DF_REPOSITORY (
				REPO_NAME, CLASS, LABEL, CREATE_USER, DESCRIPTION,
				CREATE_TIME, UPDATE_TIME, STATUS
				) values (
				?, ?, ?, ?, ?,
				'%s', '%s', ? )`,
		nowstr, nowstr)
	_, err := db.Exec(sqlstr,
		repositoryInfo.RepoName,repositoryInfo.Class,repositoryInfo.Label,
		repositoryInfo.CreateUser,repositoryInfo.Description,repositoryInfo.Status)
	return err

}

func QueryRepoList(db *sql.DB, class, label, reponame, orderBy string,
offset int64, limit int)(int64, []*Repository, error) {

	logger.Debug("QueryRepoList begin")

	sqlParams := make([]interface{}, 0, 4)
	sqlwhere := ""
	if class != "" {
		if sqlwhere == "" {
			sqlwhere = "CLASS=?"
		} else {
			sqlwhere = sqlwhere + " and CLASS=?"
		}
		sqlParams = append(sqlParams, class)
	}

	if label != "" {
		if sqlwhere == "" {
			sqlwhere = "LABEL=?"
		} else {
			sqlwhere = sqlwhere + " and LABEL=?"
		}
		sqlParams = append(sqlParams, label)
	}

	if reponame != "" {
		if sqlwhere == "" {
			sqlwhere = "REPO_NAME=?"
		} else {
			sqlwhere = sqlwhere + " and REPO_NAME=?"
		}
		sqlParams = append(sqlParams, reponame)
	}

	if sqlwhere == "" {
		sqlwhere = "STATUS=?"
	}else{
		sqlwhere = sqlwhere + " and STATUS=?"
	}
	sqlParams = append(sqlParams, "A")

	sqlorder := ""
	if orderBy != "" {
		sqlorder = fmt.Sprintf(" order by %s", orderBy)
	}

	count, err := queryRepoCount(db, sqlwhere, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return 0, nil, err
	}
	validateOffsetAndLimit(count, &offset, &limit)

	repos, err := queryRepos(db,
		sqlwhere, sqlorder,
		limit, offset, sqlParams...)

	if err != nil {
		logger.Error(err.Error())
		return 0, nil, err
	}
	return count, repos, nil
}

func QueryRepo(db *sql.DB, reponame string)(*Repository, error) {
	logger.Debug("QueryRepoList begin")
	repo := new(Repository)

	err := db.QueryRow(`SELECT
		REPO_ID,
		REPO_NAME,
		CREATE_USER,
		DESCRIPTION
		FROM DF_REPOSITORY
		WHERE
		REPO_NAME=? AND STATUS = ?`,
		reponame,"A").Scan(
		&repo.RepoId,
		&repo.RepoName,
		&repo.CreateUser,
		&repo.Description)

	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	return repo, nil
}

func QueryItemList(db *sql.DB, reponame string)([]*Dataitem, error) {
	logger.Debug("QueryItemList begin")

	sqlParams := make([]interface{}, 0, 2)
	sqlwhere := "REPO_NAME=? AND STATUS = ?"
	sqlorder := "ORDER BY CREATE_TIME"

	sqlParams = append(sqlParams,reponame)
	sqlParams = append(sqlParams,"A")

	//count, err := queryItemCount(db, sqlwhere, sqlParams...)
	items, err := queryItems(db,sqlwhere,sqlorder,sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	return items, nil
}

func QueryItem(db *sql.DB, repoName,itemName string)(*Dataitem, error) {
	logger.Debug("QueryRepoList begin")
	item := new(Dataitem)


	err := db.QueryRow(`SELECT ITEM_ID,ITEM_NAME,URL,UPDATE_TIME,SIMPLE
		FROM DF_DATAITEM
		WHERE
		REPO_NAME=? AND ITEM_NAME=? AND STATUS = ?`,
		repoName,itemName,"A").Scan(
		&item.ItemId,
		&item.ItemName,
		&item.Url,
		&item.UpdateName,
		&item.Simple)

	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	return item, nil
}

func QueryAttrList(db *sql.DB, itemId int)([]*Attribute, error) {
	logger.Debug("QueryAttrList begin")

	sqlParams := make([]interface{}, 0, 2)
	sqlwhere := "ITEM_ID=?"
	sqlorder := "ORDER BY ORDER_ID"

	sqlParams = append(sqlParams,itemId)

	attrs, err := queryAttrs(db,sqlwhere,sqlorder,sqlParams...)

	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	return attrs, nil
}

func queryRepoCount(db *sql.DB, sqlwhere string, sqlParams ...interface{}) (int64, error) {

	count := int64(0)

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`select COUNT(*) from DF_REPOSITORY %s `, sqlwhereall)
	logger.Debug(">>>\n"+
		"	%s", sqlstr)
	err := db.QueryRow(sqlstr, sqlParams...).Scan(&count)

	return count, err
}

func queryItemCount(db *sql.DB, sqlwhere string, sqlParams ...interface{}) (int64, error) {

	count := int64(0)

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`select COUNT(*) from DF_DATAITEM %s `, sqlwhereall)
	logger.Debug(">>>\n"+
		"	%s", sqlstr)
	err := db.QueryRow(sqlstr, sqlParams...).Scan(&count)

	return count, err
}

func queryRepos(db *sql.DB, sqlwhere, sqlorder string,limit int,
	offset int64, sqlParams ...interface{}) ([]*Repository, error) {

	logger.Info("Model begin queryRepos")
	defer logger.Info("Model end queryRepos")

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("WHERE %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`SELECT REPO_ID, REPO_NAME,
		CLASS, LABEL, DESCRIPTION
		FROM DF_REPOSITORY
		%s
		%s
		LIMIT %d OFFSET %d`,
		sqlwhereall,
		sqlorder,
		limit, offset)

	logger.Info(">>> %v", sqlstr)
	rows, err := db.Query(sqlstr, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	defer rows.Close()

	repos := make([]*Repository, 0, 32)
	for rows.Next() {
		repo := &Repository{}
		err := rows.Scan(&repo.RepoId, &repo.RepoName, &repo.Class, &repo.Label,&repo.Description)
		if err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}



	return repos, nil
}

func queryItems(db *sql.DB, sqlwhere, sqlorder string,sqlParams ...interface{}) ([]*Dataitem, error) {

	logger.Info("Model begin queryRepos")
	defer logger.Info("Model end queryRepos")

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`SELECT ITEM_ID,ITEM_NAME,URL
		FROM DF_DATAITEM
		%s
		%s`,
		sqlwhereall,
		sqlorder)

	logger.Info(">>> %v", sqlstr)
	rows, err := db.Query(sqlstr, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	defer rows.Close()

	items := make([]*Dataitem, 0, 32)
	for rows.Next() {
		item := &Dataitem{}
		err := rows.Scan(&item.ItemId,&item.ItemName, &item.Url)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func queryAttrs(db *sql.DB, sqlwhere, sqlorder string,sqlParams ...interface{}) ([]*Attribute, error) {

	logger.Info("Model begin queryAttrs")
	defer logger.Info("Model end queryAttrs")

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`SELECT ATTR_NAME,INSTRUCTION,ORDER_ID
		FROM DF_ATTRIBUTE
		%s
		%s`,
		sqlwhereall,
		sqlorder)

	logger.Info(">>> %v", sqlstr)
	rows, err := db.Query(sqlstr, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	defer rows.Close()

	attrs := make([]*Attribute, 0, 32)
	for rows.Next() {
		attr := &Attribute{}
		err := rows.Scan(&attr.ATTR_NAME,&attr.INSTRUCTION,&attr.ORDER_ID)
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return attrs, nil
}
