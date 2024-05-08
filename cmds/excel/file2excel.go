package excel

import (
    "dataToolBox/cmds/common"
    "dataToolBox/excel"
    "github.com/spf13/cobra"
    "log"
)

var File2ExcelCmdParam excel.TransFile2ExcelParam
var File2ExcelCmd = &cobra.Command{
    Use:   "file2Excel",
    Short: "将csv，tsv 或者带有行列分隔符的文本例类型文件转为xlsx /  Convert CSV, TSV, or text files with row-column separators to XLSX",
    Run: func(cmd *cobra.Command, args []string) {
        err := File2ExcelCmdParam.TransFile2Excel()
        if err != nil {
            log.Fatal("cmd get an err response:", err)
        } else {
            log.Println("DONE")
        }
    },
}

func init() {
    common.InitParams(File2ExcelCmd, &File2ExcelCmdParam)
}
