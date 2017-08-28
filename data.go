package main

import (
    "encoding/json"
    "errors"
    "github.com/valyala/fasthttp"
    "strings"
)

func unmarshal(body []byte, value interface{}) (error) {
    // https://gist.github.com/aodin/9493190

    // unmarshal
    err := json.Unmarshal(body, &value)
    if err != nil {
        return err
    }

    // check for 'null'
    // https://golang.org/pkg/encoding/json/#Unmarshal
    // The JSON null value unmarshals into an interface, map, pointer, or slice
    // by setting that Go value to nil. Because null is often used in JSON to mean
    // “not present,” unmarshaling a JSON null into any other Go type has no effect
    // on the value and produces no error.
    if strings.Contains(string(body), ": null") {
        return errors.New("null found")
    }

    return nil
}

func WriteInt(ctx *fasthttp.RequestCtx, n int) {
    buf := [11]byte{}
    pos := len(buf)
    i := int64(n)
    signed := i < 0
    if signed {
        i = -i
    }
    for {
        pos--
        buf[pos], i = '0'+byte(i%10), i/10
        if i == 0 {
            if signed {
                pos--
                buf[pos] = '-'
            }
            ctx.Write(buf[pos:])
            return
        }
    }
}
