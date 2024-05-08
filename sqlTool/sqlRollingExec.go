package sqlTool

import (
    "dataToolBox/common/utils"
    "errors"
    "fmt"
    "log"
    "strings"
    "time"
)

type SqlRollingExecParam struct {
    DB
    Sql       string `json:"sql" comment:"* sql，注意必带limit，且变更行需要不能再次被查到，eg  \" update xxx set status =2 where status =1  limit 100 \""`
    FSql      string `json:"fSql" comment:"Sql文件名，可以通过文件传入Sql内容"`
    SleepDeg  int64  `json:"sleepDeg" comment:"* 执行sql的间隔，毫秒，默认无，适用存在主从延迟"`
    sleepDegD time.Duration
    taskName  string
}

func (p *SqlRollingExecParam) init() (err error, closeResource func()) {
    // p.TextResFile.Check(p.taskName)

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
    col := strings.Count(strings.ToLower(p.Sql), "limit")
    if col != 1 {
        err = errors.New("illegal sql , no limit statement ")
        return
    }

    p.sleepDegD = time.Duration(p.SleepDeg * int64(time.Millisecond))

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

func (p *SqlRollingExecParam) SqlRollingExec(taskTag string) (taskErr error) {
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

    totalAffectd := int64(0)
    for {
        res, err := p.Db.Exec(p.Sql)
        if err != nil {
            taskErr = errors.New("execute sql err,sql: " + p.Sql)
            return
        }
        affected, err := res.RowsAffected()
        if err != nil {
            taskErr = errors.New("execute sql get RowsAffected err,sql: " + p.Sql)
            return
        }
        if affected == 0 {
            break
        }
        totalAffectd += affected
        if p.SleepDeg > 0 {
            time.Sleep(p.sleepDegD)
        }
        fmt.Println("affected,totalAffectd", affected, totalAffectd)
    }
    return
}
