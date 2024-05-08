package processFuncByLine

import (
    "dataToolBox/cmds/common"
    "dataToolBox/processFileByline"
    "github.com/spf13/cobra"
    "log"
)

var processFileByLineParam processFileByline.ProcessFileByLineParam
var PprocessFileByLineCmd = &cobra.Command{
    Use:   "processFileByLine",
    Short: "适用以sql模版和 文件id列表作为主要条件构造查询的sql，sql模板可以包含其它固定条件 \n\t\teg: ./dataToolBox  processFileByLine   --withRes   --file ./tmp/a.txt  --funcName  strToUpper \n\t\t    ./dataToolBox  processFileByLine   --file ./tmp/a.txt  --funcName  printLineLen",
    Run: func(cmd *cobra.Command, args []string) {
        err := processFileByLineParam.ProcessFileByLine()
        if err != nil {
            log.Fatal("cmd get an err response:", err)
        } else {
            log.Println("DONE")
        }
    },
}

func init() {
    common.InitParams(PprocessFileByLineCmd, &processFileByLineParam)

}
