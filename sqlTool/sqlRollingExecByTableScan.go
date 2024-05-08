package sqlTool

import (
    "dataToolBox/common/utils"
    "errors"
    "fmt"
    "log"
    "strconv"
    "strings"
)

type SqlRollingExecByTableScanParam struct {
    taskName string
    DB
    Sql                   string `json:"sql" comment:"* sql，注意 where最好不带主键条件，不能带 where之后的语法（limit grop order等），eg  \" update xxx set status =2 where status = 1  \""`
    Pk                    string `json:"pk" comment:"* 表主键字段名, 默认为 id "`
    ScanRange             string `json:"scanRange" comment:"扫表id范围, 默认空，即扫全表，格式类似  123~879 "`
    Limit                 int64  `json:"limit" comment:"limit（scan方式sql中的imit要写在这里）" `
    ScanRangePreQueryCond string `json:"scanRangePreQueryCond" comment:"扫表id范围预检查询, 默认空，即扫全表，格式类似  status=1   (适用目标记录集中在某个id区间，可用一个能覆盖索引的条件缩减扫描范围，两个条件都满足才适合设置)"`
    RollingLimit          int    `json:"rollingLimit" comment:"单此扫描行数，默认10000"`
    tableName             string
    sqlWithWhere          bool
}

func (p *SqlRollingExecByTableScanParam) init() (err error, closeResource func()) {
    // p.TextResFile.Check(p.taskName)

    if p.Sql == "" {
        err = errors.New("empty sql")
        return
    }

    if p.Pk == "" {
        p.Pk = "id"
    }

    if p.RollingLimit == 0 {
        p.RollingLimit = 10000
    }

    sqlLowwer := strings.ToLower(p.Sql)
    p.sqlWithWhere = strings.Contains(sqlLowwer, " where ")
    idx1 := strings.Index(sqlLowwer, "update") + 6
    idx2 := strings.Index(sqlLowwer, "set")
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

    closeResource = func() {
        p.CloseDb()
    }

    return
}

func (p *SqlRollingExecByTableScanParam) SqlRollingExecByTableScan(taskTag string) (taskErr error) {
    p.taskName = taskTag
    defer utils.TimeCost(p.taskName)()

    log.Println(p.taskName, " start")
    defer func() {
        if taskErr == nil {
            log.Println("END SUCC   ")
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

    totalAffectd := int64(0)
    curRangeBegin := minID
    curRangeEnd := curRangeBegin + p.RollingLimit - 1
    var curSql string
    for curRangeBegin <= maxID {
        if p.sqlWithWhere {
            curSql = p.Sql + fmt.Sprintf(" AND %s between  %d and  %d limit %d", p.Pk, curRangeBegin, curRangeEnd, p.RollingLimit)
        } else {
            curSql = p.Sql + fmt.Sprintf(" WHERE  %s between  %d and  %d limit %d", p.Pk, curRangeBegin, curRangeEnd, p.RollingLimit)
        }

        res, err := p.Db.Exec(curSql)
        if err != nil {
            taskErr = errors.New("execute sql err,sql: " + curSql)
            return
        }
        affected, err := res.RowsAffected()
        if err != nil {
            taskErr = errors.New("execute sql get RowsAffected err,sql: " + curSql)
            return
        }
        totalAffectd += affected
        fmt.Printf("affected %d  totalAffectd %d  ,sql: %s \n", affected, totalAffectd, curSql) // sql,affected,totalAffectd
        if p.Limit != 0 && totalAffectd > p.Limit {
            fmt.Println("totalAffectd > Limit, break", totalAffectd, " ", p.Limit)
        }
        curRangeBegin = curRangeEnd + 1
        curRangeEnd = curRangeBegin + p.RollingLimit - 1
    }

    return
}
