package processFileByline

import (
    "fmt"
    "strings"
)

func init() {
    // insert your funcs here

    // demo
    ProcessFileByLineFuncAndSaveNameMap["strToUpper"] = func(line string) (res string, err error) {
        res = strings.ToUpper(line)
        return
    }
    // demo
    ProcessFileByLineFuncNameMap["printLineLen"] = func(line string) (err error) {
        fmt.Println(len(line))
        return
    }
}
