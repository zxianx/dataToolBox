package generalRolingSelect

//
//import (
//    "bufio"
//    "dataToolBox/common"
//    "database/sql"
//    "encoding/json"
//    "errors"
//    "fmt"
//    "io"
//    "os"
//    "strconv"
//    "strings"
//    "time"
//)
//
//type SqlSelectRollingType int
//
//const (
//    SqlSelectTypeIdxRangeScan SqlSelectRollingType = 1
//    SqlSelectTypeScanTable    SqlSelectRollingType = 2 // 主键扫描
//    SqlSelectTypePagination   SqlSelectRollingType = 3 //
//    SqlSelectTypeByIdSet      SqlSelectRollingType = 4 // 在id集合内过滤，id可以是非主键
//)
//
//type SqlSelectRollingCommonConf struct {
//    Type SqlSelectRollingType `json:"type" yaml:"type"`
//
//    ////  结果存储
//    // 列分隔符，默认行按\n分割，列按空格分割， 如过saveColSep等于"US"，那么行分隔符自动设为 "RS"  即（0x1e 0x1f）
//    SaveColSep     string `json:"saveColSep" yaml:"saveColSep"`
//    TargetFilePath string `json:"targetFilePath" yaml:"targetFilePath"`
//
//    //  数据表信息
//    DbServerName string `json:"dbServerName" yaml:"dbServerName" binding:"required"` //eg tikumis dcflow
//    Dsn          string `json:"dsn" yaml:"dsn"`
//    TableName    string `json:"tableName" yaml:"tableName"`
//    PkName       string `json:"pkName" yaml:"pkName"` // default id
//
//    // 查询条件
//    Select          string `json:"select" yaml:"select"`                   // eg  a,b   *
//    WhereCond       string `json:"whereCond" yaml:"whereCond"`             // eg   a=2 and c=3
//    Limit           int    `json:"limit" yaml:"limit"`                     // 0为不限制
//    ForceIndex      string `json:"forceIndex" yaml:"forceIndex"`           // 推荐先用默认查
//    RollingPerLimit int    `json:"rollingPerLimit" yaml:"rollingPerLimit"` //default 10000  适用type 1  2 3
//
//    // SqlSelectTypeIdxRangeScan 特有参数
//    IdxRangeSelect bool   `json:"idxRangeSelect" yaml:"idxRangeSelect"`
//    RangeKeyName   string `json:"rangeKeyName" yaml:"rangeKeyName"`
//    RangeKeyUnique bool   `json:"rangeKeyUnique" yaml:"rangeKeyUnique"` //OrFewDupOk
//    RangeBegin     int64  `json:"rangeBegin" yaml:"rangeBegin"`
//    RangeEnd       int64  `json:"rangeEnd" yaml:"rangeEnd"`
//
//    // SqlSelectTypeScanTable 特有参数
//    ScanIdRangePreQueryCond string                                `json:"scanIdRangePreQueryCond" yaml:"scanIdRangePreQueryCond"`
//    RollingIdRangeFilter    func(min, max int64) (res [][2]int64) `json:"-"`
//
//    // SqlSelectTypeByIdSet 特有参数
//    IdSetFile   string `json:"idSetFile" yaml:"idSetFile"`
//    IdSetIdName string `json:"idSetIdName" yaml:"idSetIdName"`
//
//    // 内部
//    saveLineSep        string
//    selectNoRollingKey bool
//    rollingKey         string
//    // orderDesc bool
//    orderStm  string
//    idxStm    string
//    descOrder bool //当前仅用于 索引范围查询 逆序
//
//    // resource
//    db            *sql.DB
//    resFileWriter *bufio.Writer
//}
//
//func (sConf *SqlSelectRollingCommonConf) String() (res string) {
//    tmp, err := json.Marshal(sConf)
//    if err != nil {
//        fmt.Println("SqlSelectRollingCommonConf.String() err", err)
//        return
//    }
//    res = string(tmp)
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) beforeRun() (errMsg string, cleanResource func()) {
//    errMsg = sConf.check()
//    if errMsg != "" {
//        return
//    }
//    errMsg, cleanResource = sConf.initResource()
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) check() (errMsg string) {
//
//    if sConf.TargetFilePath == "" {
//        errMsg = "no  TargetFilePath"
//        return
//    }
//
//    if sConf.PkName == "" {
//        sConf.PkName = "id"
//    }
//
//    if sConf.ForceIndex != "" {
//        sConf.idxStm = fmt.Sprintf(` force index( %s )`, sConf.ForceIndex)
//    }
//
//    if sConf.IdxRangeSelect {
//        sConf.rollingKey = sConf.RangeKeyName
//
//        if sConf.RangeBegin == 0 {
//            errMsg = "no rangeBegin"
//            return
//        }
//        if sConf.RangeEnd == 0 {
//            errMsg = "no  rangeEnd"
//            return
//        }
//        if sConf.RangeBegin > sConf.RangeEnd {
//            sConf.descOrder = true
//        }
//
//    } else {
//        sConf.rollingKey = sConf.PkName
//    }
//
//    if sConf.rollingKey == "" {
//        errMsg = "no rolling key"
//        return
//    }
//
//    sConf.orderStm = sConf.rollingKey
//
//    if sConf.descOrder {
//        sConf.orderStm += " desc"
//    }
//
//    if sConf.RollingPerLimit == 0 {
//        sConf.RollingPerLimit = 10000
//    }
//
//    if sConf.Limit != 0 && sConf.RollingPerLimit > sConf.Limit {
//        sConf.RollingPerLimit = sConf.Limit
//    }
//
//    sConf.Select = strings.TrimSpace(sConf.Select)
//    if sConf.Select == "" {
//        sConf.Select = "*"
//    }
//
//    if sConf.Select != "*" && sConf.Type != SqlSelectTypeByIdSet {
//        selectsArr := strings.Split(sConf.Select, ",")
//        selectsReal := []string{sConf.rollingKey}
//        hasRoolingKey := false
//        for _, s := range selectsArr {
//            if s == "" {
//                continue
//            }
//            if s == sConf.rollingKey {
//                hasRoolingKey = true
//                continue
//            }
//            selectsReal = append(selectsReal, s)
//        }
//        sConf.Select = strings.Join(selectsReal, ",")
//        sConf.selectNoRollingKey = !hasRoolingKey
//    }
//
//    sConf.saveLineSep = "\n"
//    if sConf.SaveColSep == "" {
//        sConf.SaveColSep = " "
//    } else if sConf.SaveColSep == "US" {
//        sConf.SaveColSep = common.ColSeparatorUs
//        sConf.saveLineSep = common.LineSeparatorRs
//    }
//
//    if sConf.TargetFilePath == "" {
//        sConf.TargetFilePath = "/data/test/src/db2fOutPut" + strconv.Itoa(int(time.Now().Unix()))
//    }
//
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) initResource() (errMsg string, cleanResource func()) {
//    // run
//    var err error
//    dsn := sConf.Dsn
//    if dsn == "" {
//        dbConf := conf.RConf.Mysql[sConf.DbServerName]
//        dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", dbConf.User, dbConf.Password, dbConf.Addr, dbConf.DataBase)
//    }
//    sConf.db, err = sql.Open("mysql", dsn)
//    if err != nil {
//        errMsg = "sql.Open " + err.Error()
//        return
//    }
//
//    f, err := os.OpenFile(sConf.TargetFilePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
//    if err != nil {
//        sConf.TargetFilePath = ".db2fOutPut" + strconv.Itoa(int(time.Now().Unix()))
//        f, err = os.OpenFile(sConf.TargetFilePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
//        if err != nil {
//            errMsg = "OpenTargetFile " + err.Error()
//            return
//        }
//    }
//    fWr := io.Writer(f)
//    sConf.resFileWriter = bufio.NewWriter(fWr)
//    cleanResource = func() {
//        _ = sConf.resFileWriter.Flush()
//        _ = f.Close()
//        _ = sConf.db.Close()
//    }
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) RollingDbSelectAsFileByIdOrder(ctx *gin.Context) (resLen int, errMsg string) {
//    errMsg, clean := sConf.beforeRun()
//    if errMsg != "" {
//        errMsg = "RollingDbSelectAsFileByIdOrder beforeRun " + errMsg
//        return
//    }
//    defer clean()
//
//    whereQuery := ""
//    lastMaxId := "0"
//    sumRealGet := 0
//    for {
//        if sConf.WhereCond == "" {
//            whereQuery = fmt.Sprintf("where %s > %s", sConf.rollingKey, lastMaxId)
//        } else {
//            whereQuery = fmt.Sprintf("where ( %s ) and %s > %s", sConf.WhereCond, sConf.rollingKey, lastMaxId)
//        }
//        sqlStm := fmt.Sprintf("select %s from %s %s %s  order by %s limit %d", sConf.Select, sConf.TableName, sConf.ForceIndex, whereQuery, sConf.orderStm, sConf.RollingPerLimit)
//        rows, err := sConf.db.Query(sqlStm)
//        if err != nil {
//            errMsg = "exec Sql " + sqlStm + err.Error()
//            zlog.Error(ctx, errMsg)
//            return
//        }
//        defer rows.Close()
//        columns, err := rows.Columns()
//        if err != nil {
//            errMsg = "rows.Columns " + err.Error()
//            zlog.Error(ctx, errMsg)
//            return
//        }
//        cl := len(columns)
//
//        values := make([]sql.RawBytes, cl)
//        scanArgs := make([]interface{}, cl)
//        wline := make([]string, cl)
//        for i := range values {
//            scanArgs[i] = &values[i]
//        }
//        realGet := 0
//
//        if !sConf.selectNoRollingKey {
//            for rows.Next() {
//                err = rows.Scan(scanArgs...)
//                if err != nil {
//                    errMsg = "rows.Next " + err.Error()
//                    zlog.Error(ctx, errMsg)
//                    return
//                }
//                realGet++
//                for i := 0; i < len(values); i++ {
//                    wline[i] = string(values[i])
//                    //tmp,_:=strconv.Atoi(wline[i])
//                    //wline[i],_=utils.EncodeQid(tmp,0)
//                }
//                lastMaxId = string(values[0])
//                _, err = sConf.resFileWriter.WriteString(strings.Join(wline, sConf.SaveColSep) + sConf.saveLineSep)
//                if err != nil {
//                    errMsg = " rows.Next sConf.resFileWriter WriteString err" + err.Error()
//                    zlog.Error(ctx, errMsg)
//                    return
//                }
//            }
//        } else {
//            for rows.Next() {
//                err = rows.Scan(scanArgs...)
//                if err != nil {
//                    errMsg = "rows.Next Scan" + err.Error()
//                    zlog.Error(ctx, errMsg)
//                    return
//                }
//                realGet++
//                for i := 1; i < len(values); i++ {
//                    wline[i] = string(values[i])
//                }
//                lastMaxId = string(values[0])
//                _, err = sConf.resFileWriter.WriteString(strings.Join(wline[1:], sConf.SaveColSep) + sConf.saveLineSep)
//                if err != nil {
//                    errMsg = " rows.Next sConf.resFileWriter WriteString err" + err.Error()
//                    zlog.Error(ctx, errMsg)
//                    return
//                }
//            }
//        }
//
//        if err = rows.Err(); err != nil {
//            errMsg = "rows.ERR() " + err.Error()
//            zlog.Error(ctx, errMsg)
//            return
//        }
//        sumRealGet += realGet
//        zlog.Info(ctx, "maxId now", lastMaxId, "sumRealGet ", sumRealGet)
//        if realGet == 0 {
//            break
//        }
//        if sConf.Limit > 0 {
//            if sumRealGet >= sConf.Limit {
//                break
//            }
//
//            dec := sConf.Limit - sumRealGet
//            if dec < sConf.RollingPerLimit {
//                sConf.RollingPerLimit = dec
//                if dec == 0 {
//                    break
//                }
//            }
//        }
//    }
//    resLen = sumRealGet
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) RollingDbSelectAsFileByIdxRange(ctx *gin.Context) (resLen int, errMsg string) {
//
//    // run
//    errMsg, clean := sConf.beforeRun()
//    if errMsg != "" {
//        errMsg = "RollingDbSelectAsFileByIdOrder beforeRun " + errMsg
//        return
//    }
//    defer clean()
//
//    whereQuery := ""
//    sumRealGet := 0
//    lastMaxId := strconv.FormatInt(sConf.RangeBegin, 10)
//    for {
//        if sConf.descOrder {
//            whereQuery = fmt.Sprintf("where %s <=  %s  and %s >=  %d ", sConf.rollingKey, lastMaxId, sConf.rollingKey, sConf.RangeEnd)
//        } else {
//            whereQuery = fmt.Sprintf("where %s >=  %s  and %s <=  %d ", sConf.rollingKey, lastMaxId, sConf.rollingKey, sConf.RangeEnd)
//        }
//
//        if sConf.WhereCond != "" {
//            whereQuery += fmt.Sprintf("and ( %s ) ", sConf.WhereCond)
//        }
//        sqlStm := fmt.Sprintf("select %s from %s %s %s  order by %s limit %d", sConf.Select, sConf.TableName, sConf.ForceIndex, whereQuery, sConf.orderStm, sConf.RollingPerLimit)
//        rows, err := sConf.db.Query(sqlStm)
//        if err != nil {
//            errMsg = "exec Sql " + sqlStm + err.Error()
//            zlog.Error(ctx, errMsg)
//            return
//        }
//        // defer rows.Close()
//        columns, err := rows.Columns()
//        if err != nil {
//            errMsg = "rows.Columns " + err.Error()
//            zlog.Error(ctx, errMsg)
//            return
//        }
//        cl := len(columns)
//
//        values := make([]sql.RawBytes, cl)
//        scanArgs := make([]interface{}, cl)
//        wline := make([]string, cl)
//        for i := range values {
//            scanArgs[i] = &values[i]
//        }
//        realGet := 0
//
//        if sConf.RangeKeyUnique {
//            batchFirstId := ""
//            if !sConf.selectNoRollingKey {
//                for rows.Next() {
//                    err = rows.Scan(scanArgs...)
//                    if err != nil {
//                        errMsg = "rows.Next Scan" + err.Error()
//                        zlog.Error(ctx, errMsg)
//                        return
//                    }
//                    realGet++
//                    for i := 0; i < len(values); i++ {
//                        wline[i] = string(values[i])
//                    }
//                    lastMaxId = string(values[0])
//                    if batchFirstId == "" {
//                        batchFirstId = lastMaxId
//                    }
//                    _, err = sConf.resFileWriter.WriteString(strings.Join(wline, sConf.SaveColSep) + sConf.saveLineSep)
//                    if err != nil {
//                        errMsg = " rows.Next sConf.resFileWriter WriteString err" + err.Error()
//                        zlog.Error(ctx, errMsg)
//                        return
//                    }
//                }
//            } else {
//                for rows.Next() {
//                    err = rows.Scan(scanArgs...)
//                    if err != nil {
//                        errMsg = "rows.Next Scan" + err.Error()
//                        zlog.Error(ctx, errMsg)
//                        return
//                    }
//                    realGet++
//                    for i := 1; i < len(values); i++ {
//                        wline[i] = string(values[i])
//                    }
//                    lastMaxId = string(values[0])
//                    if batchFirstId == "" {
//                        batchFirstId = lastMaxId
//                    }
//                    _, err = sConf.resFileWriter.WriteString(strings.Join(wline[1:], sConf.SaveColSep) + sConf.saveLineSep)
//                    if err != nil {
//                        errMsg = " rows.Next sConf.resFileWriter WriteString err" + err.Error()
//                        zlog.Error(ctx, errMsg)
//                        return
//                    }
//                }
//            }
//
//            if realGet == sConf.RollingPerLimit {
//                if batchFirstId == lastMaxId {
//                    errMsg = "一次查询获取的rolling key 全部一致，请检查配置 或者 扩大rollingPerLimit"
//                    zlog.Error(ctx, errMsg)
//                    return
//                }
//            } else if realGet < sConf.RollingPerLimit {
//                sumRealGet += realGet
//                realGet = 0
//            }
//
//        } else {
//            saveLineList := make([]string, 0) //用 [][]string 错下面scanArgs 会不断覆盖，非uniqueKey时每批只存最后一行
//            lastIdList := make([]string, 0)
//            if !sConf.selectNoRollingKey {
//                for rows.Next() {
//                    err = rows.Scan(scanArgs...)
//                    if err != nil {
//                        errMsg = "rows.Next Scan" + err.Error()
//                        zlog.Error(ctx, errMsg)
//                        return
//                    }
//                    realGet++
//                    for i := 0; i < len(values); i++ {
//                        wline[i] = string(values[i])
//                    }
//                    lastIdList = append(lastIdList, string(values[0]))
//                    saveLineList = append(saveLineList, strings.Join(wline, sConf.SaveColSep))
//                }
//            } else {
//                for rows.Next() {
//                    err = rows.Scan(scanArgs...)
//                    if err != nil {
//                        errMsg = "rows.Next Scan" + err.Error()
//                        zlog.Error(ctx, errMsg)
//                        return
//                    }
//                    realGet++
//                    for i := 1; i < len(values); i++ {
//                        wline[i] = string(values[i])
//                    }
//                    lastIdList = append(lastIdList, string(values[0]))
//                    saveLineList = append(saveLineList, strings.Join(wline[1:], sConf.SaveColSep))
//                }
//            }
//
//            oriLen := len(lastIdList)
//            right := realGet - 1
//            if right >= 0 {
//                lastMaxId = lastIdList[right]
//                for ; right-1 >= 0; right-- {
//                    if lastIdList[right-1] != lastMaxId {
//                        saveLineList = saveLineList[:right]
//                        realGet = right
//                        break
//                    }
//                }
//                if realGet == oriLen && realGet < sConf.RollingPerLimit {
//                    zlog.Warn(ctx, "WARN  单次rolling结果索引值一致，若未结束，可能丢失部分数据，此时需增大RollingPerLimit太小")
//                    lastMaxIdInt, _ := strconv.Atoi(lastMaxId)
//                    lastMaxId = strconv.Itoa(lastMaxIdInt + 1)
//                }
//            }
//            for _, aline := range saveLineList {
//                _, err = sConf.resFileWriter.WriteString(aline + sConf.saveLineSep)
//                if err != nil {
//                    errMsg = " rows.Next sConf.resFileWriter WriteString err" + err.Error()
//                    return
//                }
//            }
//        }
//
//        if err = rows.Err(); err != nil {
//            errMsg = "rows.ERR() " + err.Error()
//            zlog.Error(ctx, errMsg, sqlStm)
//            err = nil
//            //   return
//        }
//        sumRealGet += realGet
//        zlog.Info(ctx, "maxId now", lastMaxId, "sumRealGet ", sumRealGet)
//        if realGet == 0 {
//            break
//        }
//        if sConf.Limit > 0 {
//            dec := sConf.Limit - sumRealGet
//            if dec < sConf.RollingPerLimit {
//                sConf.RollingPerLimit = dec
//                if dec == 0 {
//                    break
//                }
//            }
//        }
//    }
//
//    resLen = sumRealGet
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) RollingDbSelectAsFileByIdxAllScan(ctx *gin.Context) (resLen int, errMsg string) {
//
//    errMsg, clean := sConf.beforeRun()
//    if errMsg != "" {
//        errMsg = "RollingDbSelectAsFileByIdOrder beforeRun " + errMsg
//        return
//    }
//    defer clean()
//    succParseRowsNum := 0
//    errParseRowsNum := 0
//
//    if sConf.RangeEnd == 0 {
//        var maxID, minID int
//        if sConf.ScanIdRangePreQueryCond != "" {
//            sConf.ScanIdRangePreQueryCond = "where " + sConf.ScanIdRangePreQueryCond
//        }
//        preIdRangeQuerySql := fmt.Sprintf("SELECT MAX(%s), MIN(%s) FROM %s %s", sConf.PkName, sConf.PkName, sConf.TableName, sConf.ScanIdRangePreQueryCond)
//        fmt.Println("debug preIdRangeQuerySql:", preIdRangeQuerySql)
//        errTmp := sConf.db.QueryRow(preIdRangeQuerySql).Scan(&maxID, &minID)
//        if errTmp != nil {
//            if strings.Contains(errTmp.Error(), "converting NULL to int") {
//                return
//            } else {
//                errMsg = "ErrRollingDbSelectAsFileByIdxAllScan db.QueryRow max min " + errTmp.Error()
//                zlog.Error(ctx, errMsg)
//                return
//            }
//
//        }
//        sConf.RangeBegin = int64(minID)
//        sConf.RangeEnd = int64(maxID)
//        zlog.Info(ctx, "RollingDbSelectAsFileByIdxAllScan", "auto set RangeBegin RangeEnd ", sConf.TableName, minID, maxID)
//    }
//
//    whereQuery := ""
//    rollingDeg := int64(sConf.RollingPerLimit)
//    lastMaxId := sConf.RangeBegin
//    sumRealGet := 0
//    var idCond string
//    for ; lastMaxId <= sConf.RangeEnd; lastMaxId += rollingDeg {
//        if sConf.RollingIdRangeFilter == nil {
//            idCond = fmt.Sprintf(" %s between %d and %d", sConf.rollingKey, lastMaxId, lastMaxId+rollingDeg-1)
//        } else {
//            idFilterRange := sConf.RollingIdRangeFilter(lastMaxId, lastMaxId+rollingDeg-1)
//            L := len(idFilterRange)
//            if L == 0 {
//                continue
//            }
//            idRangeP := make([]string, L, L)
//            for i := 0; i < L; i++ {
//                idRangeP[i] = fmt.Sprintf(" %s between %d and %d", sConf.rollingKey, idFilterRange[i][0], idFilterRange[i][1])
//            }
//            idCond = "( " + strings.Join(idRangeP, " or ") + " ) "
//        }
//        if sConf.WhereCond == "" {
//            whereQuery = " where " + idCond
//        } else {
//            whereQuery = fmt.Sprintf("where ( %s ) and %s ", sConf.WhereCond, idCond)
//        }
//        sqlStm := fmt.Sprintf("select %s from %s %s %s  order by %s limit %d", sConf.Select, sConf.TableName, sConf.ForceIndex, whereQuery, sConf.orderStm, sConf.RollingPerLimit)
//
//        rows, err := sConf.db.Query(sqlStm)
//        if err != nil {
//            for i := 0; err != nil && i < 3; i++ {
//                rows, err = sConf.db.Query(sqlStm)
//                time.Sleep(10 * time.Second)
//            }
//            if err != nil {
//                errMsg = "ErrRollingDbSelectAsFileByIdxAllScan ExecSql " + sqlStm + err.Error()
//                zlog.Error(ctx, errMsg)
//                return
//            }
//        }
//
//        realGet, errMsg2 := sConf.handleRows(rows)
//        if errMsg2 != "" {
//            errParseRowsNum++
//            zlog.Error(ctx, errMsg2, sqlStm)
//            if errParseRowsNum > 10 && errParseRowsNum > succParseRowsNum {
//                errMsg = "ErrRollingDbSelectAsFileByIdxAllScan parseRows too many erros " + sqlStm + errMsg2
//                zlog.Error(ctx, errMsg)
//                return
//            }
//        } else {
//            succParseRowsNum++
//        }
//
//        sumRealGet += realGet
//        zlog.Info(ctx, "maxId now", lastMaxId, realGet, "sumRealGet ", sumRealGet, sqlStm)
//        if sConf.Limit > 0 && sumRealGet >= sConf.Limit {
//            zlog.Info(ctx, "sumRealGet  ,limit , stop", sumRealGet, sConf.Limit)
//            break
//        }
//    }
//    resLen = sumRealGet
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) RollingDbSelectAsFileByIdSetFile(ctx *gin.Context) (resLen int, errMsg string) {
//    errMsg, clean := sConf.beforeRun()
//    if errMsg != "" {
//        errMsg = "RollingDbSelectAsFileByIdOrder beforeRun " + errMsg
//        return
//    }
//    defer clean()
//    var sqlTemplate string
//    if sConf.WhereCond != "" {
//        sqlTemplate = fmt.Sprintf(
//            "select %s from %s where ( %s ) and %s in (%%s) ",
//            sConf.Select,
//            sConf.TableName,
//            sConf.WhereCond,
//            sConf.IdSetIdName,
//        )
//    } else {
//        sqlTemplate = fmt.Sprintf(
//            "select %s from %s where  %s in (%%s) ",
//            sConf.Select,
//            sConf.TableName,
//            sConf.IdSetIdName,
//        )
//    }
//
//    sqlTemplate = strings.TrimSpace(sqlTemplate)
//    colNum, isStar := utils.CheckSqlSelectColNum(sqlTemplate)
//    if isStar || colNum == 0 {
//        errMsg = "unSupport select *  OR  illegal selects"
//        return
//    }
//    col := strings.Count(sqlTemplate, "%s")
//    if col != 1 {
//        errMsg = "解析占位符失败 num!=1 "
//        return
//    }
//    if len(sqlTemplate) < 7 || strings.ToUpper(sqlTemplate[:6]) != "SELECT" {
//        errMsg = "illegal template"
//        return
//    }
//
//    f := func(line string) (res string, err error) {
//        ids := "'" + strings.ReplaceAll(line, "\n", `','`) + "'"
//        sqlQuery := fmt.Sprintf(sqlTemplate, ids)
//        // fmt.Println(sqlQuery)
//        rows, err := sConf.db.Query(sqlQuery)
//        if err != nil {
//            return "", errors.New(fmt.Sprintf("execSql[%s]err[%s]", sqlQuery, err.Error()))
//        }
//        defer rows.Close()
//
//        var resultRows []string
//        for rows.Next() {
//            var values = make([][]byte, colNum)
//            var scanArgs = make([]interface{}, colNum)
//            var wline = make([]string, colNum)
//            for i := range values {
//                scanArgs[i] = &values[i]
//            }
//            if err := rows.Scan(scanArgs...); err != nil {
//                return "", errors.New("scan error: " + err.Error())
//            }
//            for i := 0; i < colNum; i++ {
//                wline[i] = string(values[i])
//            }
//            resultRows = append(resultRows, strings.Join(wline, sConf.SaveColSep))
//        }
//        res = strings.Join(resultRows, sConf.saveLineSep)
//        resLen += len(resultRows)
//        return
//    }
//
//    err := fileDataProcess.ProcessFileByLineAndSaveParallel(sConf.IdSetFile, f, 20, fileDataProcess.FileProcessExt{MultiLine: 100, ResFileName: sConf.TargetFilePath,
//        ResFileLineSeperator: sConf.saveLineSep,
//        Limit:                sConf.Limit,
//    })
//    if err != nil {
//        errMsg = "common.ProcessFileByLine err:" + err.Error()
//    }
//    return
//}
//
//func (sConf *SqlSelectRollingCommonConf) RollingDbSelectAsFileByType(ctx *gin.Context) (resLen int, errMsg string) {
//    if sConf.Type == 0 {
//        sConf.Type = 2
//    }
//    fmt.Println("[RollingDbSelectAsFileByType Conf:]", sConf)
//    switch sConf.Type {
//    case SqlSelectTypeIdxRangeScan:
//        sConf.IdxRangeSelect = true
//        return sConf.RollingDbSelectAsFileByIdxRange(ctx)
//    case SqlSelectTypeScanTable:
//        return sConf.RollingDbSelectAsFileByIdxAllScan(ctx)
//    case SqlSelectTypePagination:
//        return sConf.RollingDbSelectAsFileByIdOrder(ctx)
//    case SqlSelectTypeByIdSet:
//        return sConf.RollingDbSelectAsFileByIdSetFile(ctx)
//    default:
//        errMsg = "illegal RollingDbSelect type"
//    }
//    return
//}
//
//func GetShardingSubPartitionRange(deg, idRangeMin, idRangeMax, totalPart, part int64) (filteredRanges [][2]int64) {
//
//    if deg == 0 {
//        deg = 18000 // sea
//    }
//    filteredRanges = make([][2]int64, 0)
//
//    i := idRangeMin / deg * deg
//    for {
//        l := i
//        if idRangeMin > l {
//            l = idRangeMin
//        }
//        r := i + deg - 1
//        if idRangeMax < r {
//            r = idRangeMax
//        }
//        if ((l/deg)+1)%totalPart+1 == part {
//            filteredRanges = append(filteredRanges, [2]int64{l, r})
//        }
//        if r == idRangeMax {
//            break
//        }
//        i += deg
//    }
//
//    return filteredRanges
//}
//
//func (sConf *SqlSelectRollingCommonConf) handleRows(rows *sql.Rows) (realGet int, errMsg string) {
//    defer rows.Close()
//    columns, err := rows.Columns()
//    if err != nil {
//        errMsg = "rowsColumns " + err.Error()
//        return
//    }
//    cl := len(columns)
//
//    values := make([]sql.RawBytes, cl)
//    scanArgs := make([]interface{}, cl)
//    wline := make([]string, cl)
//    for i := range values {
//        scanArgs[i] = &values[i]
//    }
//    for rows.Next() {
//        err = rows.Scan(scanArgs...)
//        if err != nil {
//            errMsg = "rows.Scan" + err.Error()
//            return
//        }
//        realGet++
//        ii := 0
//        if sConf.selectNoRollingKey {
//            ii++
//        }
//        for i := ii; i < len(values); i++ {
//            wline[i] = string(values[i])
//        }
//        _, err = sConf.resFileWriter.WriteString(strings.Join(wline[ii:], sConf.SaveColSep) + sConf.saveLineSep)
//        if err != nil {
//            errMsg = "WriteRes1 " + err.Error()
//            return
//        }
//    }
//    if err = rows.Err(); err != nil {
//        errMsg = "EndRows.ERR " + err.Error()
//        return
//    }
//    return
//}
