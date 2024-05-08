package redisCli

import (
    redigo "github.com/gomodule/redigo/redis"
    "log"
    "time"
)

// 日志打印Do args部分支持的最大长度
const logForRedisValue = 50
const prefix = "@@redis."

type RedisConf struct {
    Service         string        `yaml:"service"`
    Addr            string        `yaml:"addr"`
    Password        string        `yaml:"password"`
    MaxIdle         int           `yaml:"maxIdle"`
    MaxActive       int           `yaml:"maxActive"`
    IdleTimeout     time.Duration `yaml:"idleTimeout"`
    MaxConnLifetime time.Duration `yaml:"maxConnLifetime"`
    ConnTimeOut     time.Duration `yaml:"connTimeOut"`
    ReadTimeOut     time.Duration `yaml:"readTimeOut"`
    WriteTimeOut    time.Duration `yaml:"writeTimeOut"`
}

func (conf *RedisConf) checkConf() {

    if conf.MaxIdle == 0 {
        conf.MaxIdle = 50
    }
    if conf.MaxActive == 0 {
        conf.MaxActive = 100
    }
    if conf.IdleTimeout == 0 {
        conf.IdleTimeout = 5 * time.Minute
    }
    if conf.MaxConnLifetime == 0 {
        conf.MaxConnLifetime = 10 * time.Minute
    }
    if conf.ConnTimeOut == 0 {
        conf.ConnTimeOut = 3 * time.Second
    }
    if conf.ReadTimeOut == 0 {
        conf.ReadTimeOut = 1200 * time.Millisecond
    }
    if conf.WriteTimeOut == 0 {
        conf.WriteTimeOut = 1200 * time.Millisecond
    }
}

// 日志打印Do args部分支持的最大长度
type Redis struct {
    pool       *redigo.Pool
    service    string
    remoteAddr string
}

func InitRedisClient(conf RedisConf) (*Redis, error) {
    conf.checkConf()

    p := &redigo.Pool{
        MaxIdle:         conf.MaxIdle,
        MaxActive:       conf.MaxActive,
        IdleTimeout:     conf.IdleTimeout,
        MaxConnLifetime: conf.MaxConnLifetime,
        Wait:            true,
        Dial: func() (conn redigo.Conn, e error) {
            con, err := redigo.Dial(
                "tcp",
                conf.Addr,
                redigo.DialPassword(conf.Password),
                redigo.DialConnectTimeout(conf.ConnTimeOut),
                redigo.DialReadTimeout(conf.ReadTimeOut),
                redigo.DialWriteTimeout(conf.WriteTimeOut),
            )
            if err != nil {
                return nil, err
            }
            return con, nil
        },
        TestOnBorrow: func(c redigo.Conn, t time.Time) error {
            if time.Since(t) < time.Minute {
                return nil
            }
            _, err := c.Do("PING")
            return err
        },
    }
    c := &Redis{
        service:    conf.Service,
        remoteAddr: conf.Addr,
        pool:       p,
    }

    return c, nil
}

func (r *Redis) Do(commandName string, args ...interface{}) (reply interface{}, err error) {

    conn := r.pool.Get()
    if err := conn.Err(); err != nil {
        log.Println("[ERROR] get connection error: " + err.Error())
        return reply, err
    }

    reply, err = conn.Do(commandName, args...)
    if e := conn.Close(); e != nil {
        log.Println("[WARN] connection close error: " + e.Error())
    }
    if err != nil {
        log.Println("[ERROR] " + "redis do error: " + err.Error())
    }
    return reply, err
}

func (r *Redis) Close() error {
    return r.pool.Close()
}

func (r *Redis) Stats() (inUseCount, idleCount, activeCount int) {
    stats := r.pool.Stats()
    idleCount = stats.IdleCount
    activeCount = stats.ActiveCount
    inUseCount = activeCount - idleCount
    return inUseCount, idleCount, activeCount
}
