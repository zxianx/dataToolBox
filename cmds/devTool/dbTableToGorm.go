package devTool

import (
    "dataToolBox/cmds/common"
    "dataToolBox/devTool"
    "github.com/spf13/cobra"
    "log"
)

var DbTable2GromParam devTool.Table2GromParam
var DbTable2GromCmd = &cobra.Command{
    Use:   "dbTable2Grom",
    Short: "将根据db表名 生成gorm表结构及模板代码 \n\t\t" + `eg: ./dataToolBox dbTable2Grom  --dbConfName  demodb  --tableName  xxxx`,
    Run: func(cmd *cobra.Command, args []string) {
        err := DbTable2GromParam.Run()
        if err != nil {
            log.Fatal("cmd get an err response:", err)
        } else {
            log.Println("DONE")
        }
    },
}

func init() {
    common.InitParams(DbTable2GromCmd, &DbTable2GromParam)
}
