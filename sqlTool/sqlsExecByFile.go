package sqlTool

import (
    "dataToolBox/common/utils"
    "dataToolBox/processFileByline"
    "errors"
    "fmt"
    "log"
    "strings"
)

type SqlExecByFileParam struct {
    taskName string
    DB
    Sql  string `json:"sql" comment:"* sql 模板 ，注意转义，eg    \" update  set a='\\$2' where id = \\$1 \"  "`
    FSql string `json:"fSql" comment:"Sql模板文件名，可以通过文件传入Sql模板内容"`
    TextSrcFile
    DryRun   bool `json:"dryRun" comment:"只打印实际构造前5条sql，不实际执行"`
    Parallel int  `json:"parallel" comment:"* 执行并发度，默认30"`

    BatchIdNum        int  `json:"batchIdNum" comment:"多行聚合数量,对应sql in 条件，默认0不开启，需要源文件为单列id格式"`
    BatchIdTypeString bool `json:"batchIdTypeString" comment:"多行聚合文件id是否为字符串类型，默认false 整数类型"`
}

func (p *SqlExecByFileParam) init() (err error, closeResource func()) {
    p.TextSrcFile.Check()
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

    if p.Parallel == 0 {
        p.Parallel = 30
    }

    if p.DryRun {
        if p.TextSrcFile.Limit == 0 {
            p.TextSrcFile.Limit = 5
        }
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

func (p *SqlExecByFileParam) SqlExecByFile(taskTag string) (taskErr error) {
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

    f := func(line string) (err error) {
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
            fmt.Println("TemplateReplace err:", err)
            return err
        }
        if p.DryRun {
            fmt.Println(sqlStm)
            fmt.Println("dry run stop execute")
            return
        }
        res, err := p.Db.Exec(sqlStm)
        if err != nil {
            err = errors.New("execute sql err sql: " + err.Error() + "\t" + sqlStm)
            return err
        }
        affect, _ := res.RowsAffected()
        fmt.Println(affect, sqlStm)
        return
    }

    err := processFileByline.ProcessFileByLineParallel(p.TextSrcFile.SrcFile, f, p.Parallel, processFileByline.FileProcessExt{
        Limit:     p.TextSrcFile.Limit,
        SkipLine:  p.TextSrcFile.Skip,
        MultiLine: p.BatchIdNum,
    })

    if err != nil {
        taskErr = fmt.Errorf("ProcessFileByLineParallel Err: %w", err)
    }
    return
}
