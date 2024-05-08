package sqlTool

import (
    "bufio"
    "dataToolBox/common"
    "dataToolBox/resource"
    "database/sql"
    "errors"
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "log"
    "os"
    "strings"
    "time"
)

type TextSrcFile struct {
    SrcFile     string `json:"srcFile"  comment:"* 源文件名"`
    SColSep     string `json:"sColSep"   comment:"源文件列分隔符，默认逗号"`
    SUseSepRsUs bool   `json:"sUseRsUsSep" comment:"用 0x1e 0x1f 当源文件行列分隔符"`
    Skip        int    `json:"skip" comment:"跳过行，可用于任务中断继续，或者设为1跳过csv文件标题行"`
    Limit       int    `json:"limit" comment:"限量执行行数，不含skip"`
    sRowSep     string
}

type TextResFile struct {
    ResFile      string `json:"resFile"  comment:"结果文件名，默认 /tmp/dataToolBoxOut/{subCmdName}{$timestamp}"`
    RColSep      string `json:"rColSep" comment:"结果列分隔符，默认逗号，适用文本格式输出文件"`
    RUseSepRsUs  bool   `json:"rUseRsUsSep" comment:"用0x1e 0x1f 当结果文件行列分隔符"`
    rRowSep      string
    bufferWriter *bufio.Writer
    f            *os.File
}

func (rf *TextSrcFile) Check() () {
    if rf.SUseSepRsUs {
        rf.SColSep = common.ColSeparatorUs
        rf.sRowSep = common.LineSeparatorRs
    } else {
        rf.sRowSep = "\n"
    }

    if rf.SColSep == "" {
        rf.SColSep = ","
    }
    return
}

func (rf *TextResFile) Check(autoFileTag string) () {
    if rf.ResFile == "" {
        rf.ResFile = fmt.Sprintf("%s/%s%d", common.DefaultFilePath, autoFileTag, time.Now().UnixNano())
    }
    if rf.RUseSepRsUs {
        rf.RColSep = common.ColSeparatorUs
        rf.rRowSep = common.LineSeparatorRs
    } else {
        rf.rRowSep = "\n"
    }

    if rf.RColSep == "" {
        rf.RColSep = ","
    }
    return
}

func (rf *TextResFile) WriteDbRows(rows *sql.Rows, excludeFirstCol bool) (realGet int, errMsg string) {
    defer rows.Close()
    columns, err := rows.Columns()
    if err != nil {
        errMsg = "rowsColumns " + err.Error()
        return
    }
    cl := len(columns)

    values := make([]sql.RawBytes, cl)
    scanArgs := make([]interface{}, cl)
    wline := make([]string, cl)
    for i := range values {
        scanArgs[i] = &values[i]
    }
    for rows.Next() {
        err = rows.Scan(scanArgs...)
        if err != nil {
            errMsg = "rows.Scan" + err.Error()
            return
        }
        realGet++
        ii := 0
        if excludeFirstCol {
            ii++
        }
        for i := ii; i < len(values); i++ {
            wline[i] = string(values[i])
        }
        _, err = rf.bufferWriter.WriteString(strings.Join(wline[ii:], rf.RColSep) + rf.rRowSep)
        if err != nil {
            errMsg = "WriteRes1 " + err.Error()
            return
        }
    }
    if err = rows.Err(); err != nil {
        errMsg = "EndRows.ERR " + err.Error()
        return
    }
    return
}

func (rf *TextResFile) OpenResFile() (err error) {
    f, err := os.OpenFile(rf.ResFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
    if err != nil {
        err = fmt.Errorf("打开结果文件错误：%w", err)
        return
    }
    rf.bufferWriter = bufio.NewWriter(f)
    return
}

func (rf *TextResFile) CloseResFile() {
    err := rf.bufferWriter.Flush()
    if err != nil {
        log.Println("WARN: last write flush err:", err, rf.ResFile)
    }
    rf.f.Close()
    return
}

type DB struct {
    DbConfName string `json:"dbConfName" comment:"* db配置名"`
    Dsn        string `json:"dsn"  comment:"不根据配置直接传dsn,格式为 {$User}:{$Password}@tcp({$erverAddr})/{$DataBase}?charset=utf8 " `

    Db *sql.DB `json:"-"`
}

func (db *DB) OpenDb() (err error) {
    if db.Db != nil {
        return
    }
    if db.Dsn == "" {
        dbConf, ok := resource.MysqlConfs[db.DbConfName]
        if !ok {
            err = errors.New("unknown db conf")
            return
        }
        if dbConf.User == "" || dbConf.Password == "" || dbConf.Addr == "" || dbConf.DataBase == "" {
            err = errors.New("illegal db conf")
            return
        }
        db.Dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", dbConf.User, dbConf.Password, dbConf.Addr, dbConf.DataBase)
    }
    db.Db, err = sql.Open("mysql", db.Dsn)
    return
}

func (db *DB) CloseDb() {
    if db.Db == nil {
        return
    }
    _ = db.Db.Close()
    db.Db = nil
    return
}

//type RunInfo struct {
//    begin   time.Time
//    end     time.Time
//    err     error
//    resFile string
//    taskConfJson string
//}
