package utils

import (
    "fmt"
    "time"
)

func TimeCost(tag string) func() {
    start := time.Now()
    return func() {
        tc := time.Since(start)
        fmt.Printf("time_cost %s = %v\n", tag, tc)
    }
}
