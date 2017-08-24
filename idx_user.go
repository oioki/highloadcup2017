package main

import (
    "container/list"
    "fmt"
    "sync"
)

// list of indexes 'Location' -> index
// index itself is mapping 'Visited_at' -> UsersVisits[Visit, Distance, Country, Mark, Place]
// this index is used in request /users/:id/visits
// 800 is Location
//for e := IdxUser[116].Front(); e != nil; e = e.Next() {
//    fmt.Println(e.Value)
//}
var IdxUser map[int]*list.List

var idxUserMutex sync.RWMutex

func getIdxUser(Location int) (*list.List) {
    idxUserMutex.RLock()
    iu, ok := IdxUser[Location]
    idxUserMutex.RUnlock()
    if !ok {
        // IdxUser[Location] was not existed, now creating. There were no visits to this location.
        iu = list.New()
        idxUserMutex.Lock()
        IdxUser[Location] = iu
        idxUserMutex.Unlock()
    }
    return iu
}

func UpdateIdxUser(Location int, Distance int, Country * string, Place * string) {
    iu := getIdxUser(Location)

    for e := iu.Front(); e != nil; e = e.Next() {
        idx := e.Value.(*usersVisits)

        idx.Distance = Distance
        idx.Country = *Country
        idx.Place = *Place

        idx.Raw = []byte(fmt.Sprintf("{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", idx.Mark, idx.Visited_at, idx.Place))
    }
}
