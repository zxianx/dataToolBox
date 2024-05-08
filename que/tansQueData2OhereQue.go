package que

import (
    "dataToolBox/resource"
    "fmt"
)

type TransQue2QueueParam struct {
    FromRedisServiceName string `json:"fromRedisServiceName" comment:"redis配置名"`
    FromQueKey           string `json:"fromQueKey" comment:"队列键名"`
    ToRedisServiceName   string `json:"toRedisServiceName" comment:"redis配置名"`
    ToQueKey             string `json:"toQueKey" comment:"队列键名"`
}

func (p *TransQue2QueueParam) TransQue2Queue() (err error) {

    srcRedisClient, err := resource.GetRedisCli(p.FromRedisServiceName)
    if err != nil {
        return err
    }
    targetRedisClient, err := resource.GetRedisCli(p.ToRedisServiceName)
    if err != nil {
        return err
    }

    for nu := 1; ; nu++ {
        pop, err := srcRedisClient.RPop(p.FromQueKey)
        if err != nil {
            fmt.Println("srcRedisClient.RPop Err ", err)
            return err
        }
        if pop == nil {
            fmt.Println("FINISH ")
            break
        }
        _, err = targetRedisClient.LPush(p.ToQueKey, string(pop))
        if err != nil {
            fmt.Println("argetRedisClient.LPush Err ", err)
            return err
        }
        if nu&31 == 0 {
            fmt.Println("transed", nu)
        }
    }
    return
}
