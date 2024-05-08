package utils

import (
    "io/ioutil"
    "os"
    "path/filepath"
)

func EnsurePath(path string) error {
    // 使用 filepath.Clean 来处理路径，确保它是干净的
    cleanPath := filepath.Clean(path)

    // 检查路径是否已经存在
    _, err := os.Stat(cleanPath)
    if err == nil {
        // 目录已经存在，无需进一步处理
        return nil
    }

    if os.IsNotExist(err) {
        // 目录不存在，尝试创建它及其所有必要的父目录
        err := os.MkdirAll(cleanPath, os.ModePerm)
        if err != nil {
            return err
        }
        // fmt.Printf("目录已创建：%s\n", cleanPath)
        return nil
    }

    // 发生了其他错误，返回错误信息
    return err
}

func ReadFileAll(file string) (content string, err error) {
    tmp, err := ioutil.ReadFile(file)
    if err != nil {
        return "", err
    }
    content = string(tmp)
    return
}
