package sqlTool

import (
    "dataToolBox/common/utils"
    "database/sql"
    "errors"
    "fmt"
    "log"
    "strings"
)

type SqlSelect2FileParam struct {
    DB
    Sql  string `json:"sql" comment:"* select sql ，注意转义 "`
    FSql string `json:"fSql" comment:"Sql文件名，可以通过文件传入Sql内容"`
    TextResFile
    WithTitle bool `json:"withTitle" comment:"打印查询结果的表头，默认false，只打印数据"`
    taskName  string
}

func (p *SqlSelect2FileParam) init() (err error, closeResource func()) {
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

func (p *SqlSelect2FileParam) SqlSelect2File(taskTag string) (taskErr error) {
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

    rows, err := p.Db.Query(p.Sql)
    if err != nil {
        taskErr = fmt.Errorf("execute Sql Err: %w", err)
        return
    }
    defer rows.Close()
    columns, err := rows.Columns()
    if err != nil {
        taskErr = fmt.Errorf("get rows columns Err: %w", err)
        return
    }
    cl := len(columns)

    if p.WithTitle {
        _, err = p.TextResFile.bufferWriter.WriteString(strings.Join(columns, p.TextResFile.RColSep) + p.TextResFile.rRowSep)
    }

    values := make([]sql.RawBytes, cl)
    scanArgs := make([]interface{}, cl)
    wline := make([]string, cl)
    for i := range values {
        scanArgs[i] = &values[i]
    }
    lineIdx := 0
    for rows.Next() {
        err = rows.Scan(scanArgs...)
        if err != nil {
            fmt.Println(" rows.Next err", err)
            return
        }
        for i, value := range values {
            wline[i] = string(value)
        }

        _, err = p.TextResFile.bufferWriter.WriteString(strings.Join(wline, p.TextResFile.RColSep) + p.TextResFile.rRowSep)
        if err != nil {
            taskErr = fmt.Errorf(" rows.Next bfwL WriteString err:%w", err)
            return
        }
        lineIdx++
    }
    if err = rows.Err(); err != nil {
        taskErr = fmt.Errorf(" rowsErr:%w", err)
        return
    }
    return
}
