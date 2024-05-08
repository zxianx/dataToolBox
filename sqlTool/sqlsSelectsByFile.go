package sqlTool

import (
    "dataToolBox/common/utils"
    "dataToolBox/processFileByline"
    "errors"
    "fmt"
    "log"
    "strings"
    "sync/atomic"
)

type SqlSelectByFileIdParam struct {
    taskName string
    DB
    Sql  string `json:"sql" comment:"* sql 模板 ，注意转义,暂不支持select * ，eg    \"   select a,b from tableXXX  where deleted= 0 and id = \\$1 \"  "`
    FSql string `json:"fSql" comment:"sql模板文件名，可以通过文件传入Sql模板内容"`
    TextSrcFile
    TextResFile
    Parallel          int  `json:"-"`
    BatchIdNum        int  `json:"batchIdNum" comment:"多行聚合数量,对应sql in 条件，默认0不开启，需要源文件为单列id格式"`
    BatchIdTypeString bool `json:"batchIdTypeString" comment:"多行聚合文件id是否为字符串类型，默认false 整数类型"`
    PrintSql          bool `json:"printSql"   comment:"打印实际的查询Sql,debug用"`
}

func (p *SqlSelectByFileIdParam) init() (err error, closeResource func()) {
    p.TextSrcFile.Check()
    p.TextResFile.Check(p.taskName)
    if p.Sql == "" {
        if p.FSql != "" {
            p.Sql, err = utils.ReadFileAll(p.FSql)
            if err != nil {
                err = fmt.Errorf("read sql File err : %w", err)
                return
            }
        }
    }
    if p.Sql == "" {
        err = errors.New("empty sql")
        return
    }
    col := strings.Count(p.Sql, "$")
    if col == 0 {
        err = errors.New("illegal sql template ")
        return
    }
    if p.BatchIdNum != 0 {
        p.Parallel = 20
    } else {
        p.Parallel = 50
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

func (p *SqlSelectByFileIdParam) SqlSelectByFileId(taskTag string) (taskErr error) {
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

    colNum, isStar := CheckSqlSelectColNum(p.Sql)
    if isStar || colNum == 0 {
        taskErr = errors.New("unSupport select *  OR  illegal selects")
        return
    }
    var resLen int64

    f := func(line string) (res string, err error) {

        var sqlStm string
        if p.BatchIdNum > 1 {
            if p.BatchIdTypeString {
                line = "'" + strings.ReplaceAll(line, "\n", "','") + "'"
            } else {
                line = strings.ReplaceAll(line, "\n", ",")
            }
        }

        sqlStm, err = TemplateReplace(p.Sql, line, p.TextSrcFile.SColSep)
        if err != nil {
            err = fmt.Errorf("构造查询语句 TemplateReplace err： %w", err)
            return
        }
        if p.PrintSql {
            fmt.Println(sqlStm)
        }
        rows, err := p.Db.Query(sqlStm)
        if err != nil {
            err = fmt.Errorf("构造查询语句 execSql[%s] err :[%w]", sqlStm, err)
            return
        }
        defer rows.Close()

        var resultRows []string
        for rows.Next() {
            var values = make([][]byte, colNum)
            var scanArgs = make([]interface{}, colNum)
            var wline = make([]string, colNum)
            for i := range values {
                scanArgs[i] = &values[i]
            }
            if err := rows.Scan(scanArgs...); err != nil {
                return "", errors.New("scan error: " + err.Error())
            }
            for i := 0; i < colNum; i++ {
                wline[i] = string(values[i])
            }
            resultRows = append(resultRows, strings.Join(wline, p.SColSep))
        }
        res = strings.Join(resultRows, p.rRowSep)
        atomic.AddInt64(&resLen, int64(len(resultRows)))
        return
    }
    fmt.Println("查询到 ", resLen, " 条记录")
    err := processFileByline.ProcessFileByLineAndSaveParallel(p.SrcFile, f, p.Parallel, processFileByline.FileProcessExt{
        Limit:                p.TextSrcFile.Limit,
        SkipLine:             p.TextSrcFile.Skip,
        MultiLine:            p.BatchIdNum,
        ResFileName:          p.ResFile,
        ResFileLineSeperator: p.rRowSep,
    })
    if err != nil {
        taskErr = fmt.Errorf("common.ProcessFileByLine err: %w", err)
    }

    return
}
