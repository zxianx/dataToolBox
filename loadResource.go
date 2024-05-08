package main

import (
    "dataToolBox/resource"
    "dataToolBox/resource/redisCli"
    "fmt"
    "gopkg.in/yaml.v3"
    "os"
)

const (
    path = "./resourceConf.yaml"
)

func init() {

}

func LoadConf() {

    fmt.Println("use conf file:", path)

    rcConf := struct {
        Redis map[string]redisCli.RedisConf
        Mysql map[string]resource.MysqlConf
    }{}

    if yamlFile, err := os.ReadFile(path); err != nil {
        panic(path + " get error: %v " + err.Error())
    } else if err = yaml.Unmarshal(yamlFile, &rcConf); err != nil {
        panic(path + " unmarshal error: %v" + err.Error())
    }
    resource.RedisConfs = rcConf.Redis
    resource.MysqlConfs = rcConf.Mysql
    return
}
