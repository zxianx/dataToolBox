package sqlTool

import (
    "dataToolBox/common"
    "dataToolBox/common/utils"
    "dataToolBox/excel"
    "database/sql"
    "errors"
    "fmt"
    "github.com/360EntSecGroup-Skylar/excelize"
    "log"
    "time"
)

type SqlSelect2ExcelParam struct {
    DB
    Sql        string `json:"sql" comment:"* select sql ，注意转义 "`
    FSql       string `json:"fSql" comment:"Sql文件名，可以通过文件传入Sql内容"`
    taskName   string
    TargetFile string `json:"targetFile"  comment:"结果文件名，默认 /tmp/dataToolBoxOut/{subCmdName}{$timestamp}"`
}

func (p *SqlSelect2ExcelParam) init() (err error, closeResource func()) {
    if p.TargetFile == "" {
        p.TargetFile = fmt.Sprintf("%s/%s%d", common.DefaultFilePath, p.taskName, time.Now().UnixNano())
    }
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

    closeResource = func() {
        p.CloseDb()

    }

    return
}

func (p *SqlSelect2ExcelParam) SqlSelect2Excel(taskTag string) (taskErr error) {
    hasTargetFile := p.TargetFile != ""
    p.taskName = taskTag
    defer utils.TimeCost(p.taskName)()

    log.Println(p.taskName, " start")
    defer func() {
        if taskErr == nil {
            log.Println("END SUCC  , resFile: ", p.TargetFile)
        } else {
            log.Println("END ERR : ", taskErr)
        }
    }()

    taskErr, closeResource := p.init()
    if !hasTargetFile {
        p.TargetFile += ".xlsx"
    }

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

    execlfile := excelize.NewFile()
    sheetName := "Sheet1"
    for colIndex, value := range columns {
        cellName := excel.ColumnNumberToName(colIndex+1) + "1"
        execlfile.SetCellValue(sheetName, cellName, value)
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

        for colIndex, value := range wline {
            cellName := excel.ColumnNumberToName(colIndex+1) + fmt.Sprintf("%d", lineIdx+2)
            execlfile.SetCellValue(sheetName, cellName, value)
        }
        lineIdx++
    }
    if err = rows.Err(); err != nil {
        taskErr = fmt.Errorf(" rowsErr:%w", err)
        return
    }

    err = execlfile.SaveAs(p.TargetFile)
    if err != nil {
        taskErr = fmt.Errorf("保存excel文件失败: %w", err)
        return
    }
    return
}
