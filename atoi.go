package main

import (
    "errors"
)

var ErrRange = errors.New("value out of range")
var ErrSyntax = errors.New("invalid syntax")

// Reduced implementation of Atoi, base 10 only
// https://golang.org/src/strconv/atoi.go
func Atoi(s []byte) (int, error) {
    var n int
    var err error

    for i := 0; i < len(s); i++ {
        var v byte
        d := s[i]
        switch {
        case '0' <= d && d <= '9':
            v = d - '0'
        default:
            n = 0
            err = ErrSyntax
            goto Error
        }
        n *= int(10)
        n1 := n + int(v)
        n = n1
    }

    return n, nil

    Error:
        return n, err
}
