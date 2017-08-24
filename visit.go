package main

import (
    "fmt"
    "sync"
)

type visit struct {
    Id          * int
    Location    * int
    User        * int
    Mark        * int
    Visited_at  * int

    Raw           []byte
}

var visits map[int]*visit
var visitsMutex sync.RWMutex

func getVisit(Visit int) (*visit, bool) {
    visitsMutex.RLock()
    l, err := visits[Visit]
    visitsMutex.RUnlock()
    return l, err
}

func insertRawVisit(Visit int, v * visit) {
    visitsMutex.Lock()
    visits[Visit] = v
    visitsMutex.Unlock()
    v.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"location\":%d,\"user\":%d,\"mark\":%d,\"visited_at\":%d}", Visit, *v.Location, *v.User, *v.Mark, *v.Visited_at))
}

func updateRawVisit(Visit int, v * visit) {
    v.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"location\":%d,\"user\":%d,\"mark\":%d,\"visited_at\":%d}", Visit, *v.Location, *v.User, *v.Mark, *v.Visited_at))
}
