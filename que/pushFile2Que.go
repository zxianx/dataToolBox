package que

import (
    "dataToolBox/processFileByline"
    "dataToolBox/resource"
    "errors"
    "time"
)

type PushFile2QueueParam struct {
    File             string `json:"file"  comment:"文件路径"`
    RedisServiceName string `json:"redisServiceName" comment:"redis配置名"`
    QueKey           string `json:"queKey" comment:"队列键名"`
    MaxQueLen        int    `json:"maxQueLen"  comment:"队列最大长度，默认不限制"`
    Qps              int    `json:"qps" comment:"限速"`
    FileSkip         int    `json:"fileSkip" comment:"文件跳过量" `
    FileLimit        int    `json:"fileLimit" comment:"推送限制量" `
}

func (p *PushFile2QueueParam) PushFile2Queue() (err error) {
    redisClient, err := resource.GetRedisCli(p.RedisServiceName)
    if err != nil {
        return err
    }

    if p.MaxQueLen != 0 {
        p.MaxQueLen = 10000000
    }
    a := 0
    fileItemHandleFunc := func(line string) error {
        llen := 1000000000
        a++
        if a%100 == 0 {
            for {
                llen, err = redisClient.LLen(p.QueKey)
                if err != nil {
                    return err
                }
                if llen > p.MaxQueLen {
                    time.Sleep(1 * time.Second)
                } else {
                    break
                }
            }
        }
        _, err = redisClient.LPush(p.QueKey, line)
        return err
    }

    err = processFileByline.ProcessFileByLine(p.File, fileItemHandleFunc, processFileByline.FileProcessExt{
        SkipLine:         p.FileSkip,
        Limit:            p.FileLimit,
        QpsLimit:         p.Qps,
        ShowProcessedNum: true,
    })
    if err != nil {
        err = errors.New("PushFile2Queue ProcessFileByLine err" + err.Error())
        return
    }
    return
}
