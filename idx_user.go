package main

import (
    "container/list"
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
// TODO: try to save it into *location

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

func getIdxUserLoad(Location int) (*list.List) {
    iu, ok := IdxUser[Location]
    if !ok {
        // IdxUser[Location] was not existed, now creating. There were no visits to this location.
        iu = list.New()
        IdxUser[Location] = iu
    }
    return iu
}

func UpdateIdxUser(Location int, Distance int, CountryId int, PlaceId int) {
    iu := getIdxUser(Location)

    for e := iu.Front(); e != nil; e = e.Next() {
        idx := e.Value.(*usersVisits)

        idx.Distance = Distance
        idx.CountryId = CountryId
        idx.PlaceId = PlaceId
    }
}
