package que

import (
    "dataToolBox/cmds/common"
    "dataToolBox/que"
    "github.com/spf13/cobra"
    "log"
)

var PushFile2QueParam que.PushFile2QueueParam
var PushFile2QueCmd = &cobra.Command{
    Use:   "pushFile2Que",
    Short: "将文件按行推入队列",
    Run: func(cmd *cobra.Command, args []string) {
        err := PushFile2QueParam.PushFile2Queue()
        if err != nil {
            log.Fatal("cmd get an err response:", err)
        } else {
            log.Println("DONE")
        }
    },
}

var PullQue2fileParam que.PullFileFromQueueParam
var PullQue2fileCmd = &cobra.Command{
    Use:   "pullQue2file",
    Short: "pop队列，写入文件",
    Run: func(cmd *cobra.Command, args []string) {
        err := PullQue2fileParam.PullFileFromQueue()
        if err != nil {
            log.Fatal("cmd get an err response:", err)
        } else {
            log.Println("DONE")
        }
    },
}

var TransQue2QueParam que.TransQue2QueueParam
var TransQue2QueCmd = &cobra.Command{
    Use:   "transQue2Que",
    Short: "pop一个队列并push到另一个队列",
    Run: func(cmd *cobra.Command, args []string) {
        err := TransQue2QueParam.TransQue2Queue()
        if err != nil {
            log.Fatal("cmd get an err response:", err)
        } else {
            log.Println("DONE")
        }
    },
}

func init() {
    common.InitParams(PushFile2QueCmd, &PushFile2QueParam)
    common.InitParams(PullQue2fileCmd, &PullQue2fileParam)
    common.InitParams(TransQue2QueCmd, &TransQue2QueParam)
}
