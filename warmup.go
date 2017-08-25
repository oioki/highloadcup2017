package main

import (
    "fmt"
    "github.com/valyala/fasthttp"
    "log"
    "time"
)

var req    * fasthttp.Request
var resp   * fasthttp.Response
var client * fasthttp.Client

func warmup(url string) {
    req.SetRequestURI(url)
    client.Do(req, resp)
    _ = resp.Body()
}

func warmupAll() {
    start := time.Now()
    time.Sleep(1000 * time.Millisecond)

    req = fasthttp.AcquireRequest()
    resp = fasthttp.AcquireResponse()
    client = &fasthttp.Client{}

    for k, _ := range visits {
        warmup(fmt.Sprintf("http://127.0.0.1/visits/%d", k))
    }
    log.Println("/visits/:id warmup done")

    for i := 0; i<2; i++ {
        for k, _ := range locations {
            warmup(fmt.Sprintf("http://127.0.0.1/locations/%d", k))
            warmup(fmt.Sprintf("http://127.0.0.1/locations/%d/avg", k))
        }
        log.Println("/locations/:id{,/avg} warmup done")

        for k, _ := range users {
            warmup(fmt.Sprintf("http://127.0.0.1/users/%d", k))
            warmup(fmt.Sprintf("http://127.0.0.1/users/%d/visits", k))
        }
        log.Println("/users/:id{,/visits} warmup done")
    }
    log.Printf("Warmup done in %10s\n", time.Since(start))
}