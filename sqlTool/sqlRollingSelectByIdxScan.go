package sqlTool

import (
    "dataToolBox/common/utils"
    "database/sql"
    "errors"
    "fmt"
    "log"
    "strconv"
    "strings"
)

type SqlRollingSelectByIdxRangeParam struct {
    taskName string
    DB
    TextResFile
    Sql          string `json:"sql" comment:"* sql，注意 where最好不带主键条件，不能带where之后的语法（limit grop order等），eg  \" select xxx set status =2 where status = 1  \""`
    RollingKey   string `json:"rollingKey"  comment:"* 遍历的索引字段名"`
    ScanRange    string `json:"scanRange" comment:"* 扫描索引范围,必须, 格式如 123~879（升序扫） ，879~123（降序扫）  "`
    RollingLimit int    `json:"rollingLimit" comment:"单次获取行数，默认1000"`
    Limit        int64  `json:"limit" comment:"limit（scan方式sql中的imit要写在这里）" `

    RangeKeyUnique bool `json:"rangeKeyUnique" comment:"是否唯一索引列，时的话加上这个标志能加速"`
    rangeBegin     int64
    rangeEnd       int64

    tableName          string
    sqlWithWhere       bool
    selects            string
    selectNoRollingKey bool
    orderStm           string
    descOrder          bool
}

func (p *SqlRollingSelectByIdxRangeParam) init() (err error, closeResource func()) {
    p.TextResFile.Check(p.taskName)
    if p.Sql == "" || p.ScanRange == "" || p.RollingKey == "" {
        err = errors.New("empty sql or rollingKey or ScanRange")
        return
    }

    if p.RollingLimit <= 1 {
        p.RollingLimit = 1000
    }

    a := strings.Split(p.ScanRange, "~")
    if len(a) != 2 {
        err = errors.New("illegal pkRangeConf")
        return
    }
    p.rangeBegin, err = strconv.ParseInt(a[0], 10, 64)
    if err != nil {
        err = errors.New("illegal pkRangeConf")
        return
    }
    p.rangeEnd, err = strconv.ParseInt(a[1], 10, 64)
    if err != nil {
        err = errors.New("illegal pkRangeConf")
        return
    }
    fmt.Println("preSet scanRange ", p.rangeBegin, p.rangeEnd)

    p.orderStm = p.RollingKey
    if p.rangeEnd < p.rangeBegin {
        p.orderStm += " desc"
        p.descOrder = true
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

    idx3 := strings.Index(sqlLowwer, "select") + 6
    idx4 := strings.Index(sqlLowwer, "from")
    p.selects = strings.TrimSpace(p.Sql[idx3:idx4])
    if p.selects != "*" {
        selectsArr := strings.Split(p.selects, ",")
        selectsReal := []string{p.RollingKey}
        hasRoolingKey := false
        for _, s := range selectsArr {
            if s == "" {
                continue
            }
            if s == p.RollingKey {
                hasRoolingKey = true
                continue
            }
            selectsReal = append(selectsReal, s)
        }
        newSelects := strings.Join(selectsReal, ",")
        p.Sql = strings.Replace(p.Sql, p.selects, newSelects, 1)
        p.selectNoRollingKey = !hasRoolingKey
    }

    sqlPartAfterTableName := sqlLowwer[idx1:]
    if strings.Contains(sqlPartAfterTableName, "limit") || strings.Contains(sqlPartAfterTableName, "order") || strings.Contains(sqlLowwer, "group") {
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

func (p *SqlRollingSelectByIdxRangeParam) SqlRollingSelectByIdxRange(taskTag string) (taskErr error) {
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

    sumRealGet := 0

    appendWhere := ""
    lastMaxId := strconv.FormatInt(p.rangeBegin, 10)
    sqlStm := ""
    limitPerQuery := p.RollingLimit
    end := false
    for {
        if p.descOrder {
            appendWhere = fmt.Sprintf(" %s <=  %s  and %s >=  %d ", p.RollingKey, lastMaxId, p.RollingKey, p.rangeEnd)
        } else {
            appendWhere = fmt.Sprintf(" %s >=  %s  and %s <=  %d ", p.RollingKey, lastMaxId, p.RollingKey, p.rangeEnd)
        }

        if p.sqlWithWhere {
            sqlStm = fmt.Sprintf(" %s and (%s) order by %s limit %d", p.Sql, appendWhere, p.orderStm, limitPerQuery)
        } else {
            sqlStm = fmt.Sprintf(" %s  where %s  order by %s limit %d", p.Sql, appendWhere, p.orderStm, limitPerQuery)
        }
        fmt.Println(sqlStm)
        rows, err := p.Db.Query(sqlStm)
        if err != nil {
            taskErr = fmt.Errorf("execSql[%s]err[%w]", sqlStm, err)
            return
        }
        // defer rows.Close()
        columns, err := rows.Columns()
        if err != nil {
            taskErr = fmt.Errorf("execSql[%s]rows columns err[%w]", sqlStm, err)
            return
        }
        cl := len(columns)

        values := make([]sql.RawBytes, cl)
        scanArgs := make([]interface{}, cl)
        wline := make([]string, cl)
        for i := range values {
            scanArgs[i] = &values[i]
        }
        realGet := 0
        batchFirstId := ""
        if p.RangeKeyUnique {
            if !p.selectNoRollingKey {
                for rows.Next() {
                    err = rows.Scan(scanArgs...)
                    if err != nil {
                        taskErr = fmt.Errorf("execSql[%s]rows.Scan[%w]", sqlStm, err)
                        return
                    }
                    realGet++
                    for i := 0; i < len(values); i++ {
                        wline[i] = string(values[i])
                    }
                    lastMaxId = string(values[0])
                    if batchFirstId == "" {
                        batchFirstId = lastMaxId
                        // fmt.Println("debug", batchFirstId)
                    }
                    _, err = p.TextResFile.bufferWriter.WriteString(strings.Join(wline, p.TextResFile.RColSep) + p.TextResFile.rRowSep)
                    if err != nil {
                        taskErr = fmt.Errorf("execSql[%s]WirteResFile[%w]", sqlStm, err)
                        return
                    }
                }
            } else
            {
                for rows.Next() {
                    err = rows.Scan(scanArgs...)
                    if err != nil {
                        taskErr = fmt.Errorf("execSql[%s]rows.Scan[%w]", sqlStm, err)
                        return
                    }
                    realGet++
                    for i := 1; i < len(values); i++ {
                        wline[i] = string(values[i])
                    }
                    lastMaxId = string(values[0])
                    if batchFirstId == "" {
                        batchFirstId = lastMaxId
                        // fmt.Println("debug", batchFirstId)
                    }
                    _, err = p.TextResFile.bufferWriter.WriteString(strings.Join(wline[1:], p.TextResFile.RColSep) + p.TextResFile.rRowSep)
                    if err != nil {
                        taskErr = fmt.Errorf("execSql[%s]WirteResFile[%w]", sqlStm, err)
                        return
                    }
                }
            }
            if realGet < p.RollingLimit {
                end = true
            }
        } else {
            saveLineList := make([]string, 0) //用 [][]string 错下面scanArgs 会不断覆盖，非uniqueKey时每批只存最后一行
            lastIdList := make([]string, 0)
            if !p.selectNoRollingKey {
                for rows.Next() {
                    err = rows.Scan(scanArgs...)
                    if err != nil {
                        taskErr = fmt.Errorf("execSql[%s]rows.Scan[%w]", sqlStm, err)
                        return
                    }
                    realGet++
                    for i := 0; i < len(values); i++ {
                        wline[i] = string(values[i])
                    }
                    lastIdList = append(lastIdList, string(values[0]))
                    saveLineList = append(saveLineList, strings.Join(wline, p.TextResFile.RColSep))
                }
            } else
            {
                for rows.Next() {
                    err = rows.Scan(scanArgs...)
                    if err != nil {
                        taskErr = fmt.Errorf("execSql[%s]rows.Scan[%w]", sqlStm, err)
                        return
                    }
                    realGet++
                    for i := 1; i < len(values); i++ {
                        wline[i] = string(values[i])
                    }
                    lastIdList = append(lastIdList, string(values[0]))
                    saveLineList = append(saveLineList, strings.Join(wline[1:], p.TextResFile.RColSep))
                }
            }

            if realGet < p.RollingLimit {
                end = true
            } else {
                oriLen := len(lastIdList)
                right := realGet - 1
                lastMaxId = lastIdList[right] //  right>0
                for ; right-1 >= 0; right-- {
                    if lastIdList[right-1] != lastMaxId {
                        saveLineList = saveLineList[:right]
                        realGet = right
                        lastMaxId = lastIdList[right-1]
                        break
                    }
                }
                if realGet == oriLen {
                    taskErr = errors.New("单次查询扫描列值相同")
                    log.Println("ERROR  ，单次查询扫描列值相同，请检查配置或者增大RollingPerLimit太小重试", sqlStm)
                    return
                }
            }
            // fmt.Println("debug", lastIdList, len(lastIdList), realGet, lastMaxId)
            for _, aline := range saveLineList {
                _, err = p.TextResFile.bufferWriter.WriteString(aline + p.TextResFile.rRowSep)
                if err != nil {
                    taskErr = fmt.Errorf("rows.Next p.resFileWriter WriteString err[%w]", err)
                    return
                }
            }
        }

        if err = rows.Err(); err != nil {
            log.Println("rows.ERR() err ", err)
            err = nil
            //   return
        }
        sumRealGet += realGet
        log.Println("maxIdx now", lastMaxId, "sumRealGet ", sumRealGet)
        lastMaxIdInt, _ := strconv.Atoi(lastMaxId)
        if p.descOrder {
            lastMaxId = strconv.Itoa(lastMaxIdInt - 1)
        } else {
            lastMaxId = strconv.Itoa(lastMaxIdInt + 1)
        }

        if p.Limit > 0 {
            dec := int(p.Limit) - sumRealGet
            if dec == 0 {
                end = true
            }
            if dec < p.RollingLimit {
                limitPerQuery = dec
            }
        }

        if end {
            break
        }
    }

    log.Println("查询结果总量 ", sumRealGet)

    return
}
