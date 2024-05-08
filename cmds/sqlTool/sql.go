package sqlTool

import (
    "dataToolBox/cmds/common"
    "dataToolBox/sqlTool"
    "fmt"
    "github.com/spf13/cobra"
)

var sqlRollingExecParam sqlTool.SqlRollingExecParam
var SqlRollingExecCmd = &cobra.Command{
    Use:   "sqlRollingExec",
    Short: "适用根据固定条件大量update/delete的sql 用limit分词执行，条件能较好利用索引, 需要保证部分行变更后不会被查询条件再次查到 \n\t\t" + ` eg: ./dataToolBox sqlRollingExec  --dbConfName xxxx --sql "update xxx set status=1 where status=0 limit 1000"  --sleepDeg 1000 `,
    Run: func(cmd *cobra.Command, args []string) {
        sqlRollingExecParam.SqlRollingExec("sqlRollingExec")
    },
}

var sqlRollingExecByTableScanParam sqlTool.SqlRollingExecByTableScanParam
var SqlRollingExecByTableScanCmd = &cobra.Command{
    Use:   "sqlRollingExecByScanTable",
    Short: "适用where条件复杂，索引效果差，需要遍历表大量update/delete的sql \n\t\t" + `eg: ./dataToolBox sqlRollingExecByScanTable  --dbConfName demodb --sql "update tblAbc set status=1 where status=2" `,
    Run: func(cmd *cobra.Command, args []string) {
        sqlRollingExecByTableScanParam.SqlRollingExecByTableScan("sqlRollingExecByScanTable")
    },
}

var fSqlExecParam sqlTool.SqlExecByFileParam
var FSqlExecCmd = &cobra.Command{
    Use:   "sqlExecByFile",
    Short: "适用于根据模板sql，和文件行构造sql执行 ， 文件列作为某些条件或更新字段。 \n\t\t " + `eg:  ./dataToolBox sqlExecByFile  --dbConfName xxx  --sql  "  update xxx  set a='$2' where id=$1  "  --srcFile  ./tmp/a.txt  " `,
    Run: func(cmd *cobra.Command, args []string) {
        fSqlExecParam.SqlExecByFile("sqlExecByFile")
    },
}

var sqlSelectByFileIdParam sqlTool.SqlSelectByFileIdParam
var SqlSelectByFileIdCmd = &cobra.Command{
    Use: "sqlSelectByFileId",
    Short: "适用以sql模版和 文件id列表作为主要条件构造查询的sql，sql模板可以包含其它固定条件，不支持select *, \n\t\t" + `eg:./dataToolBox sqlSelectByFileId  --srcFile ./tmp/b.txt  --sql  "select id,title from tblAladdin where id in(\$0)  and deleted=0"  --dbConfName demodb --batchIdNum 100` + `
		  ./dataToolBox sqlSelectByFileId  --srcFile ./tmp/b.txt  --sql  "select id,title from tblAladdin where id=\$0 and deleted=0"  --dbConfName demodb`,
    Run: func(cmd *cobra.Command, args []string) {
        sqlSelectByFileIdParam.SqlSelectByFileId("sqlSelectByFileId")
    },
}

var sql2fileParam sqlTool.SqlSelect2FileParam
var Sql2fileCmd = &cobra.Command{
    Use:   "sql2file",
    Short: "查询单条sql，结果写入文本文件，适用于能很好利用索引，查询结果不太大（亿级id，百万级整行）的查询  \n\t\t" + `eg: ./dataToolBox sql2file   --dbConfName demodb --sql  "select id,title from tblAladdin  limit 10" `,
    Run: func(cmd *cobra.Command, args []string) {
        sql2fileParam.SqlSelect2File("sql2file")
    },
}

var sql2excelParam sqlTool.SqlSelect2ExcelParam
var Sql2excelCmd = &cobra.Command{
    Use:   "sql2excel",
    Short: "查询单条sql，结果写入excel，适用于能很好利用索引，查询结果不太大（亿级id，百万级整行）的查询 \n\t\t" + `eg: ./dataToolBox sql2excel   --dbConfName demodb --sql  "select id,title from tblAladdin  limit 10" `,
    Run: func(cmd *cobra.Command, args []string) {
        sql2excelParam.SqlSelect2Excel("sql2excel")
    },
}

var sqlRollingSelectByTableScanParam sqlTool.SqlRollingSelectByTableScanParam
var SqlRollingSelectByTableScanCmd = &cobra.Command{
    Use:   "sqlRollingSelectByTableScan",
    Short: "适用where条件复杂，索引效果差，需要遍历表查询sql \n\t\t" + `eg: ./dataToolBox sqlRollingSelectByTableScan --dbConfName demodb --sql "select id,title from tblAladdin where deleted = 0"  `,
    Run: func(cmd *cobra.Command, args []string) {
        sqlRollingSelectByTableScanParam.SqlRollingSelectByTableScan("sqlRollingExecByScanTable")
    },
}

var sqlRollingSelectByIdxRangeParam sqlTool.SqlRollingSelectByIdxRangeParam
var SqlRollingSelectByIdxRangeCmd = &cobra.Command{
    Use:   "sqlRollingSelectByIdxRange",
    Short: "索引范围扫描，适用能较好利用索引，但是查询量过大的sql，如查询某时间范围的行 \n\t\t" + `eg ../dataToolBox sqlRollingSelectByIdxRange  --dbConfName demodb   --rollingKey  ctime   --scanRange  0~1699894993  --sql "select id from tblAladdin where deleted =0 " `,
    Run: func(cmd *cobra.Command, args []string) {
        sqlRollingSelectByIdxRangeParam.SqlRollingSelectByIdxRange("sqlRollingSelectByIdxRange")
    },
}

var sqlRollingSelectByIdOrderParam struct{}
var SqlRollingSelectByIdOrderCmd = &cobra.Command{
    Use:   "sqlRollingSelectByIdxOrder",
    Short: "深翻页查询（重构时已废弃，尽管可以用id作为翻页offset，查询巨量数据时效果不如扫全表或选个合适的索引扫描）",
    Run: func(cmd *cobra.Command, args []string) {
        fmt.Println("")
    },
}

func init() {
    common.InitParams(SqlRollingExecCmd, &sqlRollingExecParam)
    common.InitParams(SqlRollingExecByTableScanCmd, &sqlRollingExecByTableScanParam)
    common.InitParams(FSqlExecCmd, &fSqlExecParam)
    common.InitParams(SqlSelectByFileIdCmd, &sqlSelectByFileIdParam)
    common.InitParams(Sql2fileCmd, &sql2fileParam)
    common.InitParams(Sql2excelCmd, &sql2excelParam)
    common.InitParams(SqlRollingSelectByIdOrderCmd, &sqlRollingSelectByIdOrderParam)
    common.InitParams(SqlRollingSelectByTableScanCmd, &sqlRollingSelectByTableScanParam)
    common.InitParams(SqlRollingSelectByIdxRangeCmd, &sqlRollingSelectByIdxRangeParam)

}
