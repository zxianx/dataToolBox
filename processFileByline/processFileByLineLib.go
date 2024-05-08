package processFileByline

import (
    "bufio"
    "fmt"
    "io"
    "log"
    "os"
    "strings"
    "sync"
    "time"
)

/*
   ProcessFileByLine  按行处理文件的框架， 包含文件读写、并发处理、错误与结果记录等功能。
   调用者只需要告诉框架如何处理文件中的一行内容，即传入行处理函数
   行处理函数分两种
       1、 无返回值
           type ProcessFileByLineFunc func(string) error
       2、 有返回值类型、返回一个结果作为处理后的行输出到结果文件
           type ProcessFileByLineFuncAndSave func(string) (res string, err error)
       （tips：当让你也可以用 在无返回值函数用闭包特性自定义处理结果 ）
   调用方式按函数类型和是否并发处理分为四个入口
      1、 ProcessFileByLine
      2、 ProcessFileByLineAndSave
      3、 ProcessFileByLineParallel
      4、 ProcessFileByLineAndSaveParallel
   函数入口扩展参数
       一些自定义参数、见 FileProcessExt注释
   错误、结果、日志
       错误信息会打到stdout，
       出错的行本身会写入 ${srcFile}.err
       结果会写入  ${srcFile}.res （"AndSave"方式行处理返回值）
   框架同时提供了函数NameMap，你可以在项目init函数中将自己函数加进去，然后根据名字获取并使用，实现根据函数配置名处理文件。
*/

type ProcessFileByLineFunc func(string) error
type ProcessFileByLineFuncAndSave func(string) (res string, err error)

var ProcessFileByLineFuncNameMap = map[string]ProcessFileByLineFunc{"": nil}
var ProcessFileByLineFuncAndSaveNameMap = map[string]ProcessFileByLineFuncAndSave{"": nil}

// FileProcessExt  扩展控制参数/*
type FileProcessExt struct {
    SkipLine             int    `json:"skipLine" comment:"跳过行"`
    Limit                int    `json:"limit" comment:"限量执行行数，不含skip，0为不限制"`
    SrcFileLineSeperator byte   `json:"srcFileLineSeperator" comment:"源文件行分割符，默认 '\\n'"`
    QpsLimit             int    `json:"qpsLimit" comment:"限制每秒处理行数，默认0 不限制"`
    SrcFileLineTrim      string `json:"srcFileLineTrim" comment:"字符集合，默认 \"\\r\\n\""`
    ResFileName          string `json:"resFileName" comment:"默认 srcfile+\"_res\""`
    ResFileLineSeperator string `json:"resFileLineSeperator" comment:"默认 \"\\n\"，如果你的文件不是以换行符区别一条记录的需要指定这个参数"`
    ErrFileName          string `json:"errFileName" comment:"默认 srcfile+\"_err\""`
    MultiLine            int    `json:"multiLine" comment:"默认不开启，开始后处理函数的输入为结合的多行数据，输入也应该为多行输出   eg 字符串转大写函数 MultiLine=3  输入line1\\nline2\\nline3  输出需要为 LINE1\\nLINE1\\nLINE3 （注意分割符不一定与配置分隔符一致，不一定是换行，结尾不加分割符）"`
    ShowProcessedNum     bool   `json:"-" comment:"打印读取进度,每处理1024条打印下进度, 默认不打印 （兼容用，会转为ShowProcessedNumV）"`
    ShowProcessedNumV    int    `json:"showProcessedNumV" comment:"每处理多少条打印下进度, 默认1024"`
    LoopReadN            int    `json:"-" comment:"循环读N次，压测用，仅并发读生效"`
}

func (e *FileProcessExt) check(srcFile string) {
    if e.Limit <= 0 {
        e.Limit = 1000000000
    }
    if e.SrcFileLineSeperator == 0 {
        e.SrcFileLineSeperator = '\n'
    }
    if e.SrcFileLineTrim == "" {
        e.SrcFileLineTrim = string(e.SrcFileLineSeperator)
    }
    if e.ResFileLineSeperator == "" {
        e.ResFileLineSeperator = "\n"
    }
    if e.ResFileName == "" {
        e.ResFileName = srcFile + "_res"
    }
    if e.ErrFileName == "" {
        e.ErrFileName = srcFile + "_err"
    }
    fmt.Printf("错误记录文件存储位置:%s", e.ErrFileName)
    if e.MultiLine == 0 {
        e.MultiLine = 1
    }
    //if e.ShowProcessedNumV == 0 && e.ShowProcessedNum {
    if e.ShowProcessedNumV == 0 {
        e.ShowProcessedNumV = 1024
    }
}

func ProcessFileByLine(fileSrc string, f ProcessFileByLineFunc, ext FileProcessExt) (err error) {
    ext.check(fileSrc)
    file1, err := os.Open(fileSrc)
    if err != nil {
        log.Println("open src file err:", err)
        return
    }
    defer file1.Close()
    rd1 := bufio.NewReader(file1)
    fNoEOF := true
    for i := 1; i <= ext.SkipLine; i++ {
        _, err2 := rd1.ReadString(ext.SrcFileLineSeperator)
        if err2 != nil {
            log.Println("skip line err:", err2)
            return err2
        }
    }
    qpsLimitStart := time.Now()
    readed := 0
    for j := ext.SkipLine + 1; fNoEOF && j <= ext.SkipLine+ext.Limit; j++ {
        if ext.ShowProcessedNumV > 0 && (ext.MultiLine > 1 || j%ext.ShowProcessedNumV == 0) {
            log.Println(j)
        }
        line, err := rd1.ReadString(ext.SrcFileLineSeperator)
        var tmp string
        for i := 0; i < ext.MultiLine-1; i++ {
            tmp, err = rd1.ReadString(ext.SrcFileLineSeperator)
            j++
            line += tmp
        }
        if err != nil {
            if err == io.EOF {
                fNoEOF = false
            } else {
                log.Println("read src file err：", err)
                return err
            }
        }
        if ext.QpsLimit > 0 {
            readed += ext.MultiLine
            if readed >= ext.QpsLimit {
                t2 := time.Now()
                if a := time.Second - t2.Sub(qpsLimitStart); a > 0 {
                    time.Sleep(a)
                }
                qpsLimitStart = t2
                readed = 0
            }
        }
        line = strings.Trim(line, ext.SrcFileLineTrim)
        if line == "" {
            continue
        }
        lineErr := f(line)
        if lineErr != nil {
            log.Println(j, "line get err:", lineErr)
        }
    }
    return
}

func ProcessFileByLineAndSave(fileSrc string, f ProcessFileByLineFuncAndSave, ext FileProcessExt) (err error) {
    ext.check(fileSrc)
    file1, err := os.Open(fileSrc)
    if err != nil {
        log.Println("open src file err:", err)
        return
    }
    defer file1.Close()
    rd1 := bufio.NewReader(file1)

    file2, err := os.OpenFile(ext.ResFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
    if err != nil {
        log.Println(err)
        return
    }
    fWr := io.Writer(file2)
    defer func() {
        file2.Close()
        fmt.Println("结果文件位置：", ext.ResFileName)
    }()
    bfwL := bufio.NewWriter(fWr)
    defer bfwL.Flush()

    fNoEOF := true
    for i := 1; i <= ext.SkipLine; i++ {
        _, err2 := rd1.ReadString(ext.SrcFileLineSeperator)
        if err2 != nil {
            log.Println("skip line err:", err2)
            return err2
        }
    }
    qpsLimitStart := time.Now()
    readed := 0
    for j := ext.SkipLine + 1; fNoEOF && j <= ext.SkipLine+ext.Limit; j++ {
        if ext.ShowProcessedNumV > 0 && (ext.MultiLine > 1 || j%ext.ShowProcessedNumV == 0) {
            log.Println(j) // 进度
        }
        line, err := rd1.ReadString(ext.SrcFileLineSeperator)
        var tmp string
        for i := 0; i < ext.MultiLine-1; i++ {
            tmp, err = rd1.ReadString(ext.SrcFileLineSeperator)
            j++
            line += tmp
        }
        if err != nil {
            if err == io.EOF {
                fNoEOF = false
            } else {
                log.Println("read src file err：", err)
                return err
            }
        }
        if ext.QpsLimit > 0 {
            readed += ext.MultiLine
            if readed >= ext.QpsLimit {
                t2 := time.Now()
                if a := time.Second - t2.Sub(qpsLimitStart); a > 0 {
                    time.Sleep(a)
                }
                qpsLimitStart = t2
                readed = 0
            }
        }
        line = strings.Trim(line, ext.SrcFileLineTrim)
        if line == "" {
            continue
        }
        res, lineErr := f(line)
        if lineErr != nil {
            log.Println(j, "line get err:", lineErr)
        } else {
            _, lineErr = bfwL.WriteString(res + ext.ResFileLineSeperator)
            if lineErr != nil {
                log.Println("writeLineRes", j, res, lineErr)
            }
        }
    }
    time.Sleep(1 * time.Second)
    return
}

func ProcessFileByLineParallel(fileSrc string, f ProcessFileByLineFunc, parallel int, ext FileProcessExt) (err error) {
    if parallel <= 1 {
        return ProcessFileByLine(fileSrc, f, ext)
    }

    ext.check(fileSrc)

    srcCh := make(chan string, parallel)
    errCh := make(chan string, parallel)
    wg := sync.WaitGroup{}

    itemProducerRun := func() (err error) {
        file1, err := os.Open(fileSrc)
        if err != nil {
            log.Println("open src file err:", err)
            return
        }
        defer file1.Close()
        rd1 := bufio.NewReader(file1)
        fNoEOF := true
        for i := 1; i <= ext.SkipLine; i++ {
            _, err2 := rd1.ReadString(ext.SrcFileLineSeperator)
            if err2 != nil {
                log.Println("skip line err:", err2)
                return err2
            }
        }
        qpsLimitStart := time.Now()
        readed := 0
        for j := ext.SkipLine + 1; fNoEOF && j <= ext.SkipLine+ext.Limit; j++ {
            if ext.ShowProcessedNumV > 0 && (ext.MultiLine > 1 || j%ext.ShowProcessedNumV == 0) {
                log.Println(j) // 进度
            }
            line, err := rd1.ReadString(ext.SrcFileLineSeperator)
            var tmp string
            for i := 0; i < ext.MultiLine-1; i++ {
                tmp, err = rd1.ReadString(ext.SrcFileLineSeperator)
                j++
                line += tmp
            }
            if err != nil {
                if err == io.EOF {
                    fNoEOF = false
                    if ext.LoopReadN > 0 {
                        ext.LoopReadN--
                        file1.Seek(0, 0)
                        rd1.Discard(rd1.Buffered())
                        fNoEOF = true
                    }
                } else {
                    log.Println("read src file err：", err)
                    return err
                }
            }
            if ext.QpsLimit > 0 {
                readed += ext.MultiLine
                if readed >= ext.QpsLimit {
                    t2 := time.Now()
                    if a := time.Second - t2.Sub(qpsLimitStart); a > 0 {
                        time.Sleep(a)
                    }
                    qpsLimitStart = t2
                    readed = 0
                }
            }
            if ext.QpsLimit > 0 {
                readed += ext.MultiLine
                if readed >= ext.QpsLimit {
                    t2 := time.Now()
                    if a := time.Second - t2.Sub(qpsLimitStart); a > 0 {
                        time.Sleep(a)
                    }
                    qpsLimitStart = t2
                    readed = 0
                }
            }
            line = strings.Trim(line, ext.SrcFileLineTrim)
            if line == "" {
                continue
            }
            srcCh <- line
        }
        return
    }

    errItemSaverRun := func() (err error) {
        file2, err := os.OpenFile(ext.ErrFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
        if err != nil {
            log.Println(err)
            return
        }
        fWr := io.Writer(file2)
        defer file2.Close()
        bfwL := bufio.NewWriter(fWr)
        defer bfwL.Flush()
        for eritem := range errCh {
            _, err = bfwL.WriteString(eritem + "\n")
            if err != nil {
                log.Println(err)
            }
        }
        return
    }

    go func() {
        err = itemProducerRun()
        if err != nil {
            log.Println("itemProducerRun err: ", err)
        }
        close(srcCh)
    }()
    go func() {
        err = errItemSaverRun()
        if err != nil {
            log.Println("errItemSaverRun err: ", err)
        }
    }()
    wg.Add(parallel)
    for i := 0; i < parallel; i++ {
        go func() {
            defer wg.Done()
            for item := range srcCh {
                err := f(item)
                if err != nil {
                    log.Println(err, item)
                    errCh <- item
                }
            }
        }()
    }
    wg.Wait()
    close(errCh)
    time.Sleep(1 * time.Second)
    return

}

func ProcessFileByLineAndSaveParallel(fileSrc string, f ProcessFileByLineFuncAndSave, parallel int, ext FileProcessExt) (err error) {
    if parallel <= 1 {
        return ProcessFileByLineAndSave(fileSrc, f, ext)
    }

    ext.check(fileSrc)

    srcCh := make(chan string, parallel)
    errCh := make(chan string, parallel)
    resCh := make(chan string, parallel)

    wg := sync.WaitGroup{}

    itemProducerRun := func() (err error) {
        file1, err := os.Open(fileSrc)
        if err != nil {
            log.Println("open src file err:", err)
            return
        }
        defer file1.Close()
        rd1 := bufio.NewReader(file1)
        fNoEOF := true
        for i := 1; i <= ext.SkipLine; i++ {
            _, err2 := rd1.ReadString(ext.SrcFileLineSeperator)
            if err2 != nil {
                log.Println("skip line err:", err2)
                return err2
            }
        }
        qpsLimitStart := time.Now()
        readed := 0
        for j := ext.SkipLine + 1; fNoEOF && j <= ext.SkipLine+ext.Limit; j++ {
            if ext.ShowProcessedNumV > 0 && (ext.MultiLine > 1 || j%ext.ShowProcessedNumV == 0) {
                log.Println(j) // 进度
            }
            line, err := rd1.ReadString(ext.SrcFileLineSeperator)
            var tmp string
            for i := 0; i < ext.MultiLine-1; i++ {
                tmp, err = rd1.ReadString(ext.SrcFileLineSeperator)
                j++
                line += tmp
            }
            if err != nil {
                if err == io.EOF {
                    fNoEOF = false
                    if ext.LoopReadN > 0 {
                        ext.LoopReadN--
                        file1.Seek(0, 0)
                        rd1.Discard(rd1.Buffered())
                        fNoEOF = true
                    }
                } else {
                    log.Println("read src file err：", err)
                    return err
                }
            }
            if ext.QpsLimit > 0 {
                readed += ext.MultiLine
                if readed >= ext.QpsLimit {
                    t2 := time.Now()
                    if a := time.Second - t2.Sub(qpsLimitStart); a > 0 {
                        time.Sleep(a)
                    }
                    qpsLimitStart = t2
                    readed = 0
                }
            }
            line = strings.Trim(line, ext.SrcFileLineTrim)
            if line == "" {
                continue
            }
            srcCh <- line
        }
        return
    }

    errItemSaverRun := func() (err error) {
        file2, err := os.OpenFile(ext.ErrFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
        if err != nil {
            log.Println(err)
            return
        }
        fWr := io.Writer(file2)
        defer file2.Close()
        bfwL := bufio.NewWriter(fWr)
        defer bfwL.Flush()
        for eritem := range errCh {
            _, err = bfwL.WriteString(eritem + "\n")
            if err != nil {
                log.Println(err)
            }
        }
        return
    }

    resItemSaverRun := func() (err error) {
        file2, err := os.OpenFile(ext.ResFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
        if err != nil {
            log.Println(err)
            return
        }
        fWr := io.Writer(file2)
        defer func() {
            file2.Close()
            fmt.Println("结果文件位置：", ext.ResFileName)
        }()
        bfwL := bufio.NewWriter(fWr)
        defer bfwL.Flush()
        for res := range resCh {
            _, err = bfwL.WriteString(res + ext.ResFileLineSeperator)
            if err != nil {
                log.Println(err)
            }
        }
        return
    }

    go func() {
        err = itemProducerRun()
        if err != nil {
            log.Println("itemProducerRun err: ", err)
        }
        close(srcCh)
    }()
    go func() {
        err = errItemSaverRun()
        if err != nil {
            log.Println("errItemSaverRun err: ", err)
        }
    }()
    go func() {
        err = resItemSaverRun()
        if err != nil {
            log.Println("errResItemSaverRun err: ", err)
        }
    }()

    wg.Add(parallel)
    for i := 0; i < parallel; i++ {
        go func() {
            defer wg.Done()
            for item := range srcCh {
                lineRes, lineErr := f(item)
                if lineErr != nil {
                    log.Println(lineErr, item)
                    errCh <- item
                } else {
                    resCh <- lineRes
                }
            }
        }()
    }
    wg.Wait()
    close(errCh)
    close(resCh)
    time.Sleep(1 * time.Second)
    return
}
