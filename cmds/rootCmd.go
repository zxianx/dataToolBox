package cmds

import (
    "dataToolBox/cmds/devTool"
    "dataToolBox/cmds/excel"
    "dataToolBox/cmds/processFuncByLine"
    "dataToolBox/cmds/que"
    "dataToolBox/cmds/sqlTool"
    "fmt"
    "github.com/spf13/cobra"
)

var RootCmd *cobra.Command

func init() {
    RootCmd = &cobra.Command{
        Use:   "",
        Short: "A dataProcess toolSet include fileProcess、sqlTool、csv-excel trans、redisQue etc...",
        Run: func(cmd *cobra.Command, args []string) {
            fmt.Println("Hello")
        },
    }
    //  RootCmd.PersistentFlags().IntVarP(&GlobalArgA, "GlobalA", "a", 0, "An integer Global flag")

    RootCmd.AddCommand(excel.File2ExcelCmd)

    RootCmd.AddCommand(que.TransQue2QueCmd)
    RootCmd.AddCommand(que.PushFile2QueCmd)
    RootCmd.AddCommand(que.PullQue2fileCmd)

    RootCmd.AddCommand(devTool.DbTable2GromCmd)

    RootCmd.AddCommand(processFuncByLine.PprocessFileByLineCmd)

    RootCmd.AddCommand(sqlTool.SqlRollingExecCmd)
    RootCmd.AddCommand(sqlTool.SqlRollingExecByTableScanCmd)
    RootCmd.AddCommand(sqlTool.FSqlExecCmd)
    RootCmd.AddCommand(sqlTool.SqlSelectByFileIdCmd)
    RootCmd.AddCommand(sqlTool.Sql2fileCmd)
    RootCmd.AddCommand(sqlTool.Sql2excelCmd)
    RootCmd.AddCommand(sqlTool.SqlRollingSelectByTableScanCmd)
    RootCmd.AddCommand(sqlTool.SqlRollingSelectByIdxRangeCmd)

}
