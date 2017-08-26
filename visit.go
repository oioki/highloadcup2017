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

type visit1 struct {
    Id          int
    Location    int
    User        int
    Mark        int
    Visited_at  int

    Raw         []byte
}

var visits map[int]*visit
var visitsMutex sync.RWMutex

const visitsMaxCount = 10000740+1000074  // +10%
var visitsCount int
var visits1[visitsMaxCount+1]visit1

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

func updateRawVisit1(Visit int) {
    visits1[Visit].Raw = []byte(fmt.Sprintf("{\"id\":%d,\"location\":%d,\"user\":%d,\"mark\":%d,\"visited_at\":%d}", Visit, visits1[Visit].Location, visits1[Visit].User, visits1[Visit].Mark, visits1[Visit].Visited_at))
}
