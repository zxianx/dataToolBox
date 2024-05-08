package devTool

import (
    "dataToolBox/sqlTool"
    "database/sql"
    "errors"
    "fmt"
    "log"
    "strings"
)

type Table2GromParam struct {
    sqlTool.DB
    TableName string `json:"tableName" comment:"表名" `
}

func (tgp *Table2GromParam) Run() (err error) {
    err = tgp.OpenDb()
    if err != nil {
        log.Println("loadDbClient Err ", err)
        return
    }
    defer tgp.CloseDb()

    t2s := &Table2Struct{
        StructNamePrefix: "Gorm",
        table:            tgp.TableName,
        enableJsonTag:    true,
        db:               tgp.Db,
    }
    _, err = t2s.Run()
    if err != nil {
        log.Println(err)
        return
    }
    return
}

const (
    pk        = "PRI"
    unique    = "UNI"
    nullAble  = "YES"
    autoincr  = "auto_increment"
    on_update = "on update CURRENT_TIMESTAMP"
)

var typeForMysqlToGo = map[string]string{
    "int":                "int",
    "integer":            "int",
    "tinyint":            "int",
    "smallint":           "int",
    "mediumint":          "int",
    "bigint":             "int64",
    "int unsigned":       "int",
    "integer unsigned":   "int",
    "tinyint unsigned":   "int",
    "smallint unsigned":  "int",
    "mediumint unsigned": "int",
    "bigint unsigned":    "int",
    "bit":                "int",
    "bool":               "bool",
    "enum":               "string",
    "set":                "string",
    "varchar":            "string",
    "char":               "string",
    "tinytext":           "string",
    "mediumtext":         "string",
    "text":               "string",
    "longtext":           "string",
    "blob":               "string",
    "tinyblob":           "string",
    "mediumblob":         "string",
    "longblob":           "string",
    "json":               "string",
    "date":               "time.Time", // time.Time or string
    "datetime":           "time.Time",
    "timestamp":          "time.Time",
    "time":               "time.Time",
    "float":              "float64",
    "double":             "float64",
    "decimal":            "float64",
    "binary":             "string",
    "varbinary":          "string",
}

type Table2Struct struct {
    dsn              string
    db               *sql.DB
    table            string
    prefix           string
    err              error
    realNameMethod   string
    enableJsonTag    bool
    StructNamePrefix string
    StructName       string
    tagKey           string
}

func NewTable2Struct() *Table2Struct {
    return &Table2Struct{}
}

func (t *Table2Struct) Dsn(d string) *Table2Struct {
    t.dsn = d
    return t
}

func (t *Table2Struct) TagKey(r string) *Table2Struct {
    t.tagKey = r
    return t
}

func (t *Table2Struct) RealNameMethod(r string) *Table2Struct {
    t.realNameMethod = r
    return t
}

func (t *Table2Struct) DB(d *sql.DB) *Table2Struct {
    t.db = d
    return t
}

func (t *Table2Struct) SetTable(tab string) *Table2Struct {
    t.table = tab
    return t
}

func (t *Table2Struct) Prefix(p string) *Table2Struct {
    t.prefix = p
    return t
}

func (t *Table2Struct) EnableJsonTag(p bool) *Table2Struct {
    t.enableJsonTag = p
    return t
}

func (t *Table2Struct) Run() (string, error) {

    t.StructName = t.StructNamePrefix + camelCase(t.table)
    t.dialMysql()
    if t.err != nil {
        return "", t.err
    }
    tableName := strings.TrimSpace(t.table)
    fmt.Println(fmt.Sprintf("\n\n ************************************************** start convert %s ************************************************** \n\n", tableName))

    // 获取表和字段的shcema  //拿多表 写了一半，伪需求
    tableColumns, err := t.getColumns(tableName)
    if err != nil {
        return "", err
    }
    var structContent string

    // 组装struct
    for _, item := range tableColumns {
        tabDepth := 1
        structContent += "type " + t.StructName + " struct {\n"
        for _, v := range item {
            comment := ""
            if v.ColumnComment != "" {
                comment = "//" + v.ColumnComment
            }
            jsonTag := ""
            if t.enableJsonTag {
                jsonTag = fmt.Sprintf(" json:\"%s\"", v.JsonTag)
            }
            structContent += fmt.Sprintf("%s%s %s `gorm:\"%s\"%s` %s\n",
                tab(tabDepth), v.ColumnKeyName, v.Type, v.GormTag, jsonTag, comment)
        }
        structContent += tab(tabDepth-1) + "}\n\n"

        structContent += `
	func (c *GORMNAME) getDb() *gorm.DB {
    return helpers.xxxxxClient
}
`

        structContent += fmt.Sprintf("func (c *%s) TableName() string {\n", t.StructName)
        structContent += fmt.Sprintf("%sreturn \"%s\"\n", tab(tabDepth), t.table)
        structContent += "}\n\n"

        structContent += `

func (c *GORMNAME) ExistByPk(ctx *gin.Context) (exist bool, err error) {

    count := 0
    err = c.getDb().WithContext(ctx).Raw("select count(1) from TABLENAME where id  = ? ", c.Id).Scan(&count).Error
    exist = count > 0
    return
}

func (c *GORMNAME) Save(ctx *gin.Context) (err error) {
   // 直接save会保留所有字段 包括空字段
    if c.Id == 0 {
		return c.Create(ctx) 
    } else {
		return c.UpdateByPk(ctx) 
    }
/*
    // 无法根据有无id判断插入更新情况
	 exist, err := c.ExistByPk(ctx)
    if err != nil {
        return err
    }
    if exist {
        return c.UpdateByPk(ctx)
    } else {
        return c.Create(ctx)
    }

*/

}

func (c *GORMNAME) GetByPk(ctx *gin.Context, selects string) (err error) {
    if c.Id==0{
        err=errors.New("empty Id")
        return
    }
    db := c.getDb()
    if ctx != nil {
        db = db.WithContext(ctx)
    }
    if selects == "" {
        selects = "*"
    }
    err = db.Table(c.TableName()).Select(selects).First(c, c.Id).Error
    return
}

func (c *GORMNAME) GetOne(ctx *gin.Context,selects , extraWhereCond , order  string) (err error) {
    db := c.getDb()
	if ctx!= nil {
		db=db.WithContext(ctx)
	}
	if selects == "" {
		selects = "*"
	}
	if  extraWhereCond != "" {
		db = db.Where( extraWhereCond)
	}
    if order != "" {
        db = db.Order(order)
    }
    err = db.Table(c.TableName()).Select(selects).Where(&c).First(&c).Error
    return 
}


func (c *GORMNAME) GetList(ctx *gin.Context,selects, extraWhereCond, order   string , limit,offset int) (res []GORMNAME, err error) {
    db := c.getDb()
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	db = db.Table(c.TableName()).Where(&c)
	if  extraWhereCond != "" {
		db = db.Where( extraWhereCond)
	}
	if selects!=""{
		db= db.Select(selects)
	}
    if limit != 0 {
        db = db.Limit(limit).Offset(offset)
        if order == "" {
            order = "id"
        }
    }
    if order != "" {
        db = db.Order(order)
    }
	err = db.Find(&res).Error
	return
}

func (c *GORMNAME) GetRowsByIds(ctx *gin.Context, idStr string,selects string) (itemList []GORMNAME, err error) {
      db := c.getDb()
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	if selects == "" {
		selects = "*"
	}
    err = db.WithContext(ctx).Where(fmt.Sprintf("id in (%s)", idStr)).Select(selects).Find(&itemList).Error
    return 
}

func (c *GORMNAME) Create(ctx *gin.Context) (err error) {
    db := c.getDb()
	if ctx != nil {
		db = db.WithContext(ctx)
	}
    c.Ctime = time.Now().Unix()
    c.Utime = time.Now().Unix()
    err = db.Model(&c).Create(&c).Error
    return 
}

// UpdateByPk  更新单条记录推荐， 避免意外参数错误Update批量错误更新
func (c *GORMNAME) UpdateByPk(ctx *gin.Context, updateFields ...string) (err error) {
    if c.Id==0{
        err = errors.New("empty id")
        return 
    }

    c.Utime = time.Now().Unix()
    db := c.getDb()
    if ctx != nil {
        db = db.WithContext(ctx)
    }
    if  len(updateFields )!=0  {
        updateFields = append(updateFields, "utime")
        db = db.Select(updateFields)
    }
	err = db.Table(c.TableName()).Where("id = ?", c.Id).Updates(&c).Error
    return 
}

func (c *GORMNAME) Updates(ctx *gin.Context, cond *GORMNAME, condRaw string, updateFields []string, limit int) (err error) {
    c.Utime = time.Now().Unix()
    db := c.getDb()
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	if  len(updateFields )!=0  {
		updateFields = append(updateFields, "utime")
		db = db.Select(updateFields)
	}
	if condRaw != "" {
		db = db.Where(condRaw)
	}
	if cond != nil {
		db = db.Where(cond)
	}
	if limit != 0 {
		db = db.Limit(limit)
	}
	err = db.Table(c.TableName()).Updates(&c).Error
	return
}



`

    }

    fmt.Println(strings.ReplaceAll(structContent, "GORMNAME", t.StructName))

    fmt.Println(fmt.Sprintf("\n\n ************************************************** end convert %s ************************************************** \n\n", tableName))

    return structContent, nil
}

func (t *Table2Struct) dialMysql() {
    if t.db == nil {
        if t.dsn == "" {
            t.err = errors.New("dsn数据库配置缺失")
            return
        }
        t.db, t.err = sql.Open("mysql", t.dsn)
    }
    return
}

type column struct {
    ColumnKeyName string
    ColumnName    string
    Key           string
    Type          string
    Nullable      string
    ColumnDefault interface{}
    TableName     string
    ColumnComment string
    ColumnType    string
    Extra         string
    GormTag       string
    JsonTag       string
}

// Function for fetching schema definition of passed table
func (t *Table2Struct) getColumns(table string) (tableColumns map[string][]column, err error) {
    tableColumns = make(map[string][]column)
    // sql
    var sqlStr = `SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,TABLE_NAME,COLUMN_COMMENT,COLUMN_TYPE,COLUMN_DEFAULT,COLUMN_KEY,EXTRA
		FROM information_schema.COLUMNS 
		WHERE table_schema = DATABASE()`
    sqlStr += fmt.Sprintf(" AND TABLE_NAME = '%s'", table)
    rows, err := t.db.Query(sqlStr)
    if err != nil {
        fmt.Println("Error reading table information: ", err.Error())
        return
    }

    defer rows.Close()
    for rows.Next() {
        col := column{}
        err = rows.Scan(&col.ColumnName, &col.Type, &col.Nullable, &col.TableName, &col.ColumnComment, &col.ColumnType, &col.ColumnDefault, &col.Key, &col.Extra)
        if err != nil {
            fmt.Println(err.Error())
            return
        }

        col.ColumnKeyName = camelCase(col.ColumnName)
        col.Type = typeForMysqlToGo[col.Type]

        col.JsonTag = strings.ToLower(col.ColumnName[0:1]) + col.ColumnName[1:]
        if col.Nullable == nullAble {
            col.JsonTag += ",omitempty"
        }
        gormTag := "column:" + col.ColumnName
        if col.Key == pk {
            gormTag = gormTag + ";primaryKey"
        }
        if col.Key == unique {
            gormTag = gormTag + ";unique"
        }
        if col.Extra == autoincr {
            gormTag = gormTag + ";autoIncrement"
        }
        //col.ColumnDefault
        if col.Nullable != nullAble {
            gormTag = gormTag + ";not null"
        }
        if col.ColumnDefault != nil {
            //fmt.Println( reflect.TypeOf(col.ColumnDefault))// all []uint8
            tmp := fmt.Sprint(string(col.ColumnDefault.([]uint8)))
            if col.Type == "string" {
                gormTag = gormTag + ";default:'" + tmp + "'"
            } else {
                gormTag = gormTag + ";default:" + tmp
            }

        }
        if col.Extra == on_update {
            gormTag = gormTag + ";autoUpdateTime"
        }

        col.GormTag = gormTag

        if _, ok := tableColumns[col.TableName]; !ok {
            tableColumns[col.TableName] = []column{}
        }
        tableColumns[col.TableName] = append(tableColumns[col.TableName], col)
    }
    return
}

func camelCase(str string) string {
    var text string
    for _, p := range strings.Split(str, "_") {
        // 字段首字母大写的同时, 是否要把其他字母转换为小写
        switch len(p) {
        case 0:
        case 1:
            text += strings.ToUpper(p[0:1])
        default:
            text += strings.ToUpper(p[0:1]) + p[1:]
        }
    }
    return text
}
func tab(tabDepth int) string {
    return strings.Repeat("\t", tabDepth)
}

/*
xorm
		gormTag := fmt.Sprintf("`%s:\"%s", "xorm", col.ColumnType)
		if col.Key == pk {
			gormTag = gormTag + " pk "
		}
		if col.Key == unique {
			gormTag = gormTag + " unique "
		}
		if col.Extra == autoincr {
			gormTag = gormTag + " autoincr "
		}
		if col.Nullable == nullAble {
			gormTag = gormTag + " null "
		} else {
			gormTag = gormTag + " notnull "
		}

		if col.Extra == on_update {
			gormTag = gormTag + " created "
		}
*/
