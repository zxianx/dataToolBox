package sqlTool

import (
    "database/sql"
    "fmt"
    "regexp"
    "strconv"
    "strings"
)

// TemplateReplace 字符串模板替换 替换模板中的占位符 占位符格式为 $数字 数字从1开始 0表示整个字符串
func TemplateReplace(template, line, sep string) (string, error) {
    // 分割line字符串
    parts := strings.Split(line, sep)

    // 构造结果字符串
    var result strings.Builder

    // 遍历模板字符串
    for i := 0; i < len(template); i++ {
        if template[i] == '$' {
            // 确保'$'后有字符
            if i+1 < len(template) && template[i+1] >= '0' && template[i+1] <= '9' {
                // 获取'$'后的数字
                numStr := ""
                j := i + 1
                for ; j < len(template) && template[j] >= '0' && template[j] <= '9'; j++ {
                    numStr += string(template[j])
                }

                // 转换数字
                num, err := strconv.Atoi(numStr)
                if err != nil {
                    return "", err
                }

                // 检查索引是否有效
                if num > len(parts) {
                    return "", fmt.Errorf("index %d out of range for line", num)
                }

                // 替换占位符
                if num == 0 {
                    result.WriteString(line)
                } else {
                    result.WriteString(parts[num-1])
                }

                // 更新i
                i = j - 1
            } else {
                // 如果'$'后没有字符，则原样输出'$'
                result.WriteByte(template[i])
            }
        } else {
            // 常规字符直接添加到结果中
            result.WriteByte(template[i])
        }
    }

    return result.String(), nil
}

func CheckSqlSelectColNum(sql string) (int, bool) {
    // 将查询语句转为小写以进行不区分大小写的匹配
    lowerSQL := strings.ToLower(sql)

    // 使用正则表达式匹配 SELECT 子句
    re := regexp.MustCompile(`\bselect\b(.*?)\bfrom\b`)
    matches := re.FindStringSubmatch(lowerSQL)

    if len(matches) >= 2 {
        columns := strings.TrimSpace(matches[1])

        // 如果列数为 "*"，返回 isStar 为 true
        if columns == "*" {
            return 0, true
        }

        // 否则，分割列数，计算列数
        colList := strings.Split(columns, ",")
        return len(colList), false
    }

    // 如果无法匹配 SELECT 子句，返回错误
    return 0, false
}

func GetShardedTableNames(db *sql.DB, tablePrefix string) ([]string, error) {
    query := fmt.Sprintf("SHOW TABLES LIKE '%s%%'", tablePrefix)
    rows, err := db.Query(query)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    tableNames := []string{}

    for rows.Next() {
        var tableName string
        err := rows.Scan(&tableName)
        if err != nil {
            return nil, err
        }

        tableNames = append(tableNames, tableName)
    }
    err = rows.Err()
    if err != nil {
        return nil, err
    }
    return tableNames, nil
}
