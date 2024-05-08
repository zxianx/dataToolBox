package utils

import "github.com/bytedance/sonic"

func JsonEncode(p any) (res string) {
    res, _ = sonic.MarshalString(p)
    return
}
