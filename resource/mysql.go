package resource

import (
    "time"
)

type MysqlConf struct {
    Service         string        `yaml:"service"`
    DataBase        string        `yaml:"database"`
    Addr            string        `yaml:"addr"`
    User            string        `yaml:"user"`
    Password        string        `yaml:"password"`
    Charset         string        `yaml:"charset"`
    MaxIdleConns    int           `yaml:"maxidleconns"`
    MaxOpenConns    int           `yaml:"maxopenconns"`
    ConnMaxIdlTime  time.Duration `yaml:"maxIdleTime"`
    ConnMaxLifeTime time.Duration `yaml:"connMaxLifeTime"`
    ConnTimeOut     time.Duration `yaml:"connTimeOut"`
    WriteTimeOut    time.Duration `yaml:"writeTimeOut"`
    ReadTimeOut     time.Duration `yaml:"readTimeOut"`
}

var MysqlConfs map[string]MysqlConf
