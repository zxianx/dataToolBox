package processFileByline

import (
    "errors"
)

type ProcessFileByLineParam struct {
    File     string `json:"file" comment:"*源文件"`
    FuncName string `json:"funcName" comment:"*内置函数名"`
    WithRes  bool   `json:"withRes" comment:"*带结果的函数类型"`
    Parallel int    `json:"parallel" comment:"处理并发度，默认1，串行，简单字符串处理不建议设置并发"`
    FileProcessExt
}

func (p *ProcessFileByLineParam) ProcessFileByLine() (taskErr error) {
    if p.WithRes {
        f := ProcessFileByLineFuncAndSaveNameMap[p.FuncName]
        if f == nil {
            taskErr = errors.New("unknow func")
            return
        }
        taskErr = ProcessFileByLineAndSaveParallel(p.File, f, p.Parallel, p.FileProcessExt)
    } else {
        f := ProcessFileByLineFuncNameMap[p.FuncName]
        if f == nil {
            taskErr = errors.New("unknow func")
            return
        }
        taskErr = ProcessFileByLineParallel(p.File, f, p.Parallel, p.FileProcessExt)
    }
    return
}
