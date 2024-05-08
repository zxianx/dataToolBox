# dataToolBox

    常用的数据处理工具，包含 各种sql工具，redis队列工具、 文件转excel、gorm代码生成 等工具，具有完整的命令行参数解释， 
    dataToolBox -h 查看命令及简单的用例demo ，
    dataToolBox cmd -h 查看详细用法,

```shell

Available Commands:
  completion                  Generate the autocompletion script for the specified shell
  dbTable2Grom                将根据db表名 生成gorm表结构及模板代码 
                eg: ./dataToolBox dbTable2Grom  --dbConfName  demodb  --tableName  xxxx
  file2Excel                  将csv，tsv 或者带有行列分隔符的文本例类型文件转为xlsx /  Convert CSV, TSV, or text files with row-column separators to XLSX
  help                        Help about any command
  processFileByLine           适用以sql模版和 文件id列表作为主要条件构造查询的sql，sql模板可以包含其它固定条件 
                eg: ./dataToolBox  processFileByLine   --withRes   --file ./tmp/a.txt  --funcName  strToUpper 
                    ./dataToolBox  processFileByLine   --file ./tmp/a.txt  --funcName  printLineLen
  pullQue2file                pop队列，写入文件
  pushFile2Que                将文件按行推入队列
  sql2excel                   查询单条sql，结果写入excel，适用于能很好利用索引，查询结果不太大（亿级id，百万级整行）的查询 
                eg: ./dataToolBox sql2excel   --dbConfName demodb --sql  "select id,title from tblAladdin  limit 10" 
  sql2file                    查询单条sql，结果写入文本文件，适用于能很好利用索引，查询结果不太大（亿级id，百万级整行）的查询  
                eg: ./dataToolBox sql2file   --dbConfName demodb --sql  "select id,title from tblAladdin  limit 10" 
  sqlExecByFile               适用于根据模板sql，和文件行构造sql执行 ， 文件列作为某些条件或更新字段。 
                 eg:  ./dataToolBox sqlExecByFile  --dbConfName xxx  --sql  "  update xxx  set a='$2' where id=$1  "  --srcFile  ./tmp/a.txt  " 
  sqlRollingExec              适用根据固定条件大量update/delete的sql 用limit分词执行，条件能较好利用索引, 需要保证部分行变更后不会被查询条件再次查到 
                 eg: ./dataToolBox sqlRollingExec  --dbConfName xxxx --sql "update xxx set status=1 where status=0 limit 1000"  --sleepDeg 1000 
  sqlRollingExecByScanTable   适用where条件复杂，索引效果差，需要遍历表大量update/delete的sql 
                eg: ./dataToolBox sqlRollingExecByScanTable  --dbConfName demodb --sql "update tblAbc set status=1 where status=2" 
  sqlRollingSelectByIdxRange  索引范围扫描，适用能较好利用索引，但是查询量过大的sql，如查询某时间范围的行 
                eg ../dataToolBox sqlRollingSelectByIdxRange  --dbConfName demodb   --rollingKey  ctime   --scanRange  0~1699894993  --sql "select id from tblAladdin where deleted =0 " 
  sqlRollingSelectByTableScan 适用where条件复杂，索引效果差，需要遍历表查询sql 
                eg: ./dataToolBox sqlRollingSelectByTableScan --dbConfName demodb --sql "select id,title from tblAladdin where deleted = 0"  
  sqlSelectByFileId           适用以sql模版和 文件id列表作为主要条件构造查询的sql，sql模板可以包含其它固定条件，不支持select *, 
                eg:./dataToolBox sqlSelectByFileId  --srcFile ./tmp/b.txt  --sql  "select id,title from tblAladdin where id in(\$0)  and deleted=0"  --dbConfName demodb --batchIdNum 100
                  ./dataToolBox sqlSelectByFileId  --srcFile ./tmp/b.txt  --sql  "select id,title from tblAladdin where id=\$0 and deleted=0"  --dbConfName demodb
  transQue2Que                pop一个队列并push到另一个队列

Flags:
  -h, --help   help for this command


```