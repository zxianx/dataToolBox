package common

import (
    "github.com/spf13/cobra"
    "log"
    "reflect"
)

func GetFieldComment(obj interface{}, fieldName string) string {
    t := reflect.TypeOf(obj)
    if t.Kind() == reflect.Ptr {
        t = t.Elem()
    }
    field, _ := t.FieldByName(fieldName)
    return field.Tag.Get("comment")
}

//InitParams 通用参数绑定
func InitParams(cmd *cobra.Command, params interface{}) {
    t := reflect.TypeOf(params).Elem()
    v := reflect.ValueOf(params).Elem()

    for i := 0; i < t.NumField(); i++ {
        field := t.Field(i)
        value := v.Field(i)

        fieldName := field.Name
        jsontag := field.Tag.Get("json")
        fieldComment := field.Tag.Get("comment")
        if jsontag == "-" {
            continue
        }
        if fieldName[0] <= 'z' && fieldName[0] >= 'a' {
            continue
        }
        if field.Anonymous && field.Type.Kind() == reflect.Struct {
            InitParams(cmd, value.Addr().Interface())
            continue
        }
        switch value.Type().Kind() {
        case reflect.String:
            cmd.Flags().StringVar(value.Addr().Interface().(*string), jsontag, "", fieldComment)
        case reflect.Int:
            cmd.Flags().IntVar(value.Addr().Interface().(*int), jsontag, 0, fieldComment)
        case reflect.Int64:
            cmd.Flags().Int64Var(value.Addr().Interface().(*int64), jsontag, 0, fieldComment)
        case reflect.Bool:
            cmd.Flags().BoolVar(value.Addr().Interface().(*bool), jsontag, false, fieldComment)
        case reflect.Uint8:
            cmd.Flags().Uint8Var(value.Addr().Interface().(*uint8), jsontag, 0, fieldComment)
        default:
            // 不支持的类型可额外手动绑定 或者增加在此其他类型的处理逻辑
            log.Println("[WARN] ,通用参数绑定不支持的字段类型，字段名 ", fieldName, " 类型 ", value.Type().Kind())
        }
    }
}
