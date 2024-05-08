package resource

import (
    "dataToolBox/resource/redisCli"
    "errors"
)

var RedisConfs map[string]redisCli.RedisConf

var redisClientNameMap map[string]*redisCli.Redis

func GetRedisCli(name string) (cli *redisCli.Redis, err error) {
    cli, ok := redisClientNameMap[name]
    if ok {
        return cli, nil
    }
    c, ok := RedisConfs[name]
    if !ok {
        err = errors.New("unknown redis server:" + name)
        return
    }
    cli, err = redisCli.InitRedisClient(c)
    return
}
