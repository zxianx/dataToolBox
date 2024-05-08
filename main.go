package main

import (
    "dataToolBox/cmds"
    "dataToolBox/common"
    "dataToolBox/common/utils"
    "fmt"
    "os"
)

func main() {
    LoadConf()
    if err := cmds.RootCmd.Execute(); err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    _ = utils.EnsurePath(common.DefaultFilePath)
}
