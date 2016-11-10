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

type Repository struct {
	RepoId      int        `json:"repoId,omitempty"`
	RepoName    string     `json:"repoName"`
	ChRepoName  string     `json:"chRepoName"`
	Class       string     `json:"class,omitempty"`
	Label       string     `json:"label,omitempty"`
	CreateUser  string     `json:"createUser,omitempty"`
	Description string     `json:"description,omitempty"`
	CreateTime  *time.Time `json:"createTime,omitempty"`
	UpdateTime  *time.Time `json:"updateTime,omitempty"`
	Status      string     `json:"status,omitempty"`
	ImageUrl    string     `json:"imageUrl,omitempty"`
}

type Dataitem struct {
	ItemId     int        `json:"itemId,omitempty"`
	ItemName   string     `json:"itemName"`
	RepoName   string     `json:"repoName"`
	Url        string     `json:"url,omitempty"`
	CreateTime *time.Time `json:"createTime,omitempty"`
	UpdateTime *time.Time `json:"updateTime,omitempty"`
	Status     string     `json:"status,omitempty"`
	Simple     string     `json:"simple,omitempty"`
}

type Attribute struct {
	AttrId      int    `json:"attrId,omitempty"`
	ItemId      int    `json:"itemId,omitempty"`
	AttrName    string `json:"attrName,omitempty"`
	Instruction string `json:"instruction,omitempty"`
	OrderId     int    `json:"orderId,omitempty"`
	Example     string `json:"example,omitempty"`
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
	case "repoid":
		return "REPO_ID"
	case "updatetime":
		return "UPDATE_TIME"
	}
	return ""
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
				REPO_NAME, CH_REPO_NAME, CLASS, LABEL, CREATE_USER, DESCRIPTION,
				CREATE_TIME, UPDATE_TIME, STATUS
				) values (
				?, ?, ?, ?, ?,
				'%s', '%s', ? )`,
		nowstr, nowstr)
	_, err := db.Exec(sqlstr,
		repositoryInfo.RepoName, repositoryInfo.ChRepoName, repositoryInfo.Class, repositoryInfo.Label,
		repositoryInfo.CreateUser, repositoryInfo.Description, repositoryInfo.Status)
	return err

}

func QueryRepoList(db *sql.DB, class, label, reponame, orderBy, sortOrder string,
	offset int64, limit int) (int64, []*Repository, error) {

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
	} else {
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

func QueryRepo(db *sql.DB, reponame string) (*Repository, error) {
	logger.Debug("QueryRepoList begin")
	repo := new(Repository)

	err := db.QueryRow(`SELECT
		REPO_ID,
		REPO_NAME,
		CH_REPO_NAME,
		CREATE_USER,
		DESCRIPTION
		FROM DF_REPOSITORY
		WHERE
		REPO_NAME=? AND STATUS = ?`,
		reponame, "A").Scan(
		&repo.RepoId,
		&repo.RepoName,
		&repo.ChRepoName,
		&repo.CreateUser,
		&repo.Description)

	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	return repo, nil
}

func QueryItemList(db *sql.DB, reponame string) ([]*Dataitem, error) {
	logger.Debug("QueryItemList begin")

	sqlParams := make([]interface{}, 0, 2)
	sqlwhere := "REPO_NAME=? AND STATUS = ?"
	sqlorder := "ORDER BY CREATE_TIME"

	sqlParams = append(sqlParams, reponame)
	sqlParams = append(sqlParams, "A")

	//count, err := queryItemCount(db, sqlwhere, sqlParams...)
	items, err := queryItems(db, sqlwhere, sqlorder, sqlParams...)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	return items, nil
}

func QueryItem(db *sql.DB, repoName, itemName string) (*Dataitem, error) {
	logger.Debug("QueryRepoList begin")
	item := new(Dataitem)

	err := db.QueryRow(`SELECT ITEM_ID,ITEM_NAME,URL,UPDATE_TIME,SIMPLE
		FROM DF_DATAITEM
		WHERE
		REPO_NAME=? AND ITEM_NAME=? AND STATUS = ?`,
		repoName, itemName, "A").Scan(
		&item.ItemId,
		&item.ItemName,
		&item.Url,
		&item.UpdateTime,
		&item.Simple)

	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	return item, nil
}

func QueryAttrList(db *sql.DB, itemId int) ([]*Attribute, error) {
	logger.Debug("QueryAttrList begin")

	sqlParams := make([]interface{}, 0, 2)
	sqlwhere := "ITEM_ID=?"
	sqlorder := "ORDER BY ORDER_ID"

	sqlParams = append(sqlParams, itemId)

	attrs, err := queryAttrs(db, sqlwhere, sqlorder, sqlParams...)

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

/*func queryItemCount(db *sql.DB, sqlwhere string, sqlParams ...interface{}) (int64, error) {

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
}*/

func queryRepos(db *sql.DB, sqlwhere, sqlorder string, limit int,
	offset int64, sqlParams ...interface{}) ([]*Repository, error) {

	logger.Info("Model begin queryRepos")
	defer logger.Info("Model end queryRepos")

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("WHERE %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`SELECT REPO_ID, REPO_NAME,
		CH_REPO_NAME, CLASS, LABEL, DESCRIPTION, IMAGE_URL
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
		err := rows.Scan(&repo.RepoId, &repo.RepoName, &repo.ChRepoName, &repo.Class, &repo.Label, &repo.Description, &repo.ImageUrl)
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

func queryItems(db *sql.DB, sqlwhere, sqlorder string, sqlParams ...interface{}) ([]*Dataitem, error) {

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
		err := rows.Scan(&item.ItemId, &item.ItemName, &item.Url)
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

func queryAttrs(db *sql.DB, sqlwhere, sqlorder string, sqlParams ...interface{}) ([]*Attribute, error) {

	logger.Info("Model begin queryAttrs")
	defer logger.Info("Model end queryAttrs")

	sqlwhereall := ""
	if sqlwhere != "" {
		sqlwhereall = fmt.Sprintf("where %s", sqlwhere)
	}
	sqlstr := fmt.Sprintf(`SELECT ATTR_NAME,INSTRUCTION,ORDER_ID,EXAMPLE
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

		err := rows.Scan(&attr.AttrName, &attr.Instruction, &attr.OrderId, &attr.Example)

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
