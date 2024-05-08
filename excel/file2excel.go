package excel

import (
    "dataToolBox/common"
    "dataToolBox/common/utils"
    "dataToolBox/processFileByline"
    "errors"
    "fmt"
    "github.com/360EntSecGroup-Skylar/excelize"
    "strconv"
    "strings"
)

/*
    TransFile2Excel
    将csv，tsv 或者带有行列分隔符的文本例类型文件转为 xlsx
   （注意： 源文件需要全部读入内存，无法处理超大文件）
*/

type TransFile2ExcelParam struct {
    SrcFile    string `json:"srcFile"    comment:"必须，源文件路径"`
    TargetFile string `json:"targetFile" comment:"默认为 ${SrcFile}.trans.xlsx，目标文件路径"`
    CosSep     string `json:"cosSep"     comment:"列分隔符，默认为 \",\""`
    RowSep     byte   `json:"rowSep"     comment:"行分隔符，默认为 \\n "`
    UseRsUsSep bool   `json:"useRsUsSep" comment:"使用 RS US 作为表格文件分隔符"`
}

func (p *TransFile2ExcelParam) checkParam() (err error) {
    if p.SrcFile == "" {
        err = errors.New("empty src file")
        return
    }
    if p.TargetFile == "" {
        p.TargetFile = p.SrcFile + ".trans.xlsx"
    }
    if p.CosSep == "" {
        p.CosSep = ","
    }
    if p.RowSep == 0 {
        p.RowSep = '\n'
    }
    if p.UseRsUsSep {
        p.RowSep = common.LineSeparatorRs[0] // "\x1E"
        p.CosSep = common.ColSeparatorUs     // "\x1F"
    }
    return
}

func (p *TransFile2ExcelParam) TransFile2Excel() (err error) {
    err = p.checkParam()
    if err != nil {
        return err
    }
    fmt.Println("checkedParam \n ", utils.JsonEncode(p))
    // 创建一个新的 Excel 文件
    xlsx := excelize.NewFile()
    sheetName := "Sheet1"
    row := 1
    f := func(line string) (err error) {
        lineArr := strings.Split(line, p.CosSep)
        for colIndex, cellValue := range lineArr {
            cellName := ColumnNumberToName(colIndex+1) + strconv.Itoa(row)
            xlsx.SetCellValue(sheetName, cellName, cellValue)
        }
        row++
        return
    }
    err = processFileByline.ProcessFileByLine(p.SrcFile, f, processFileByline.FileProcessExt{SrcFileLineSeperator: p.RowSep})
    if err != nil {
        err = errors.New("ProcessFileByLineErr " + err.Error())
        return
    }
    err = xlsx.SaveAs(p.TargetFile)
    if err != nil {
        err = errors.New("保存 Excel 文件失败: " + err.Error())
        return
    }
    return
}
