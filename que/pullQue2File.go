package que

import (
    "bufio"
    "dataToolBox/resource"
    "fmt"
    "os"
)

type PullFileFromQueueParam struct {
    File             string `json:"file"  comment:"文件路径"`
    RedisServiceName string `json:"redisServiceName" comment:"redis配置名"`
    QueKey           string `json:"queKey" comment:"队列键名"`
    Limit            int    `json:"limit" comment:"拉取量" `
    IsLPop           bool   `json:"isLPop" comment:"默认false，即默认rpop"`
}

func (p *PullFileFromQueueParam) PullFileFromQueue() (err error) {
    redisClient, err := resource.GetRedisCli(p.RedisServiceName)
    if err != nil {
        return err
    }
    if p.Limit == 0 {
        p.Limit = 10000000000
    }
    left := p.Limit
    file, err := os.OpenFile(p.File, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)
    if err != nil {
        return err
    }
    defer file.Close()
    writer := bufio.NewWriter(file)
    defer writer.Flush()
    batchNum := 2
    totalErr := 0
    for left > 0 {
        if left < batchNum {
            batchNum = left
        }
        var pulled [][]byte
        if p.IsLPop { // Pop from the left
            pulled, err = redisClient.LRange(p.QueKey, 0, batchNum-1)
            if err == nil {
                _, err = redisClient.LTrim(p.QueKey, batchNum, -1)
            }
        } else { // Pop from the right
            pulled, err = redisClient.LRange(p.QueKey, -batchNum, -1)
            if err == nil {
                _, err = redisClient.LTrim(p.QueKey, 0, -batchNum-1)
            }
        }

        // fmt.Println(left, batchNum, len(pulled), err)

        if err != nil {
            totalErr++
            fmt.Println("pull", err)
            if totalErr > 5 {
                // Handle the case when totalErr exceeds a certain threshold
                fmt.Println("pull err too many tims", err)
                return
            }
            continue
        } else {
            l := len(pulled)
            if l == 0 {
                break
            }
            left -= l
        }

        // Convert [][]byte to []string
        res := make([]string, len(pulled))
        for i, data := range pulled {
            res[i] = string(data)
        }

        // Write pulled data to the file using buffered writer
        for _, data := range res {
            _, err := writer.WriteString(data + "\n")
            if err != nil {
                fmt.Println("write", err)
                return err
            }
        }
    }

    return
}
