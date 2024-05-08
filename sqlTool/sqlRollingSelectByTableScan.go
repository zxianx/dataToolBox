package sqlTool

import (
    "dataToolBox/common/utils"
    "errors"
    "fmt"
    "log"
    "strconv"
    "strings"
)

type SqlRollingSelectByTableScanParam struct {
    taskName string
    DB
    TextResFile
    Sql                   string `json:"sql" comment:"* sql，注意where最好不带主键条件，不能带where之后的语法（limit grop order等），eg  \" select xxx set status =2 where status = 1  \""`
    Pk                    string `json:"pk" comment:"* 表主键字段名, 默认为 id "`
    ScanRange             string `json:"scanRange" comment:"扫表id范围, 默认空，即扫全表，格式类似  123~879 "`
    Limit                 int64  `json:"limit" comment:"limit（scan方式sql中的imit要写在这里）" `
    ScanRangePreQueryCond string `json:"scanRangePreQueryCond" comment:"扫表id范围预检查询, 默认空，即扫全表，格式类似  status=1   (适用目标记录集中在某个id区间，可用一个能覆盖索引的条件缩减扫描范围，两个条件都满足才适合设置)"`
    RollingLimit          int    `json:"rollingLimit" comment:"单此扫描行数，默认10000"`
    tableName             string
    sqlWithWhere          bool
    selectNoRollingKey    bool
    selects               string
    orderStm              string
}

func (p *SqlRollingSelectByTableScanParam) init() (err error, closeResource func()) {
    p.TextResFile.Check(p.taskName)
    if p.Sql == "" {
        err = errors.New("empty sql")
        return
    }

    if p.Pk == "" {
        p.Pk = "id"

    }
    p.orderStm = p.Pk

    if p.RollingLimit == 0 {
        p.RollingLimit = 10000
    }

    sqlLowwer := strings.ToLower(p.Sql)
    p.sqlWithWhere = strings.Contains(sqlLowwer, " where ")
    idx1 := strings.Index(sqlLowwer, "from") + 4
    idx2 := strings.Index(sqlLowwer, "where")
    if idx2 == -1 {
        idx2 = len(sqlLowwer)
    }

    p.tableName = strings.TrimSpace(p.Sql[idx1:idx2])
    if p.tableName == "" {
        err = errors.New("illegal sql ,cant parse p.tableName ")
        return
    }

    sqlPartAfterTableName := sqlLowwer[idx1:]
    if strings.Contains(sqlPartAfterTableName, "limit") || strings.Contains(sqlPartAfterTableName, "order") || strings.Contains(sqlLowwer, "group") || strings.Contains(sqlPartAfterTableName, p.Pk) {
        fmt.Println("WARN !!!  sql中可能包含不允许的 limit、group by、order by 语法，或者where条件包含主键条件  **** WARN")
    }

    fmt.Println("taskConf:\n", utils.JsonEncode(p))

    err = p.OpenDb()
    if err != nil {
        return
    }

    err = p.OpenResFile()
    if err != nil {
        p.CloseDb()
        return
    }

    closeResource = func() {
        p.CloseDb()
        p.CloseResFile()
    }

    return
}

func (p *SqlRollingSelectByTableScanParam) SqlRollingSelectByTableScan(taskTag string) (taskErr error) {
    p.taskName = taskTag
    defer utils.TimeCost(p.taskName)()

    log.Println(p.taskName, " start")
    defer func() {
        if taskErr == nil {
            log.Println("END SUCC  , resFile: ", p.ResFile)
        } else {
            log.Println("END ERR : ", taskErr)
        }
    }()

    taskErr, closeResource := p.init()
    if taskErr != nil {
        return
    }
    defer closeResource()

    var maxID, minID int

    if p.ScanRange != "" {
        a := strings.Split(p.ScanRange, "~")
        if len(a) != 2 {
            taskErr = fmt.Errorf("illegal pkRangeConf")
            return
        }
        var err error
        minID, err = strconv.Atoi(a[0])
        if err != nil {
            taskErr = fmt.Errorf("illegal pkRangeConf")
            return
        }
        maxID, err = strconv.Atoi(a[1])
        if err != nil {
            taskErr = fmt.Errorf("illegal pkRangeConf")
            return
        }
        fmt.Println("preSet p.PkRange ", minID, maxID)
    } else {
        if p.ScanRangePreQueryCond != "" {
            p.ScanRangePreQueryCond = "where " + p.ScanRangePreQueryCond
        }
        preQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s %s", p.Pk, p.Pk, p.tableName, p.ScanRangePreQueryCond)

        err := p.Db.QueryRow(preQuery).Scan(&minID, &maxID)
        if err != nil {
            taskErr = fmt.Errorf("preCheckScanRangeErr,sql[%s],err[%w]", preQuery, err)
            return
        }
        log.Println("Auto-p.PkRange ", preQuery, "res:", minID, maxID)
    }

    succParseRowsNum := 0
    errParseRowsNum := 0
    totolErr := 0
    sumRealGet := 0

    curRangeBegin := minID
    curRangeEnd := curRangeBegin + p.RollingLimit - 1
    var curSql string
    for ; curRangeBegin <= maxID; {
        if p.sqlWithWhere {
            curSql = p.Sql + fmt.Sprintf(" AND %s between  %d and  %d  limit %d", p.Pk, curRangeBegin, curRangeEnd, p.RollingLimit)
        } else {
            curSql = p.Sql + fmt.Sprintf(" WHERE  %s between  %d and  %d limit %d", p.Pk, curRangeBegin, curRangeEnd, p.RollingLimit)
        }

        rows, err := p.Db.Query(curSql)
        if err != nil {
            totolErr++
            if totolErr > 10 && totolErr*10 > sumRealGet {
                taskErr = fmt.Errorf("too much sql query Err, last Sql【%s】,lastErr[%w]", curSql, err)
                return
            } else {
                log.Printf("ERROR exec Sql [%s] err [%s]", curSql, err.Error())
            }
            curRangeBegin = curRangeEnd + 1
            curRangeEnd = curRangeBegin + p.RollingLimit - 1
            continue
        }

        realGet, errMsg2 := p.TextResFile.WriteDbRows(rows, false)
        if errMsg2 != "" {
            errParseRowsNum++
            if errParseRowsNum > 10 && errParseRowsNum > succParseRowsNum {
                taskErr = fmt.Errorf("too much sql query WriteDbRows Err, last Sql【%s】,lastErr[%s]", curSql, errMsg2)
                return
            } else {
                log.Printf("ERROR exec Sql [%s] WriteDbRows err [%s]", curSql, errMsg2)
            }
            curRangeBegin = curRangeEnd + 1
            curRangeEnd = curRangeBegin + p.RollingLimit - 1
            continue
        } else {
            succParseRowsNum++
        }

        sumRealGet += realGet

        fmt.Printf("GET %d SUM_GET %d SUN_ERR %d %d ||  %s \n", realGet, sumRealGet, totolErr, errParseRowsNum, curSql)
        if p.Limit != 0 && int64(sumRealGet) > p.Limit {
            fmt.Println("sumRealGet > Limit, break", sumRealGet, " ", p.Limit)
        }
        curRangeBegin = curRangeEnd + 1
        curRangeEnd = curRangeBegin + p.RollingLimit - 1
    }

    return
}
