package excel

func ColumnNumberToName(colIndex int) string {
    colName := ""
    for colIndex > 0 {
        colIndex--
        colName = string('A'+colIndex%26) + colName
        colIndex /= 26
    }
    return colName
}
