package main

import (
//    "fmt"
    "sync"
)

type visit_update struct {
    Id          * int
    Location    * int
    User        * int
    Mark        * int
    Visited_at  * int
}

type visit struct {
    Id          int
    Location    int
    User        int
    Mark        int
    Visited_at  int
}

var visits map[int]*visit
var visitsMutex sync.RWMutex

const visitsMaxCount = 10000740
var visitsCount int
var visits1[visitsMaxCount+1]visit

// Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
func getVisit(Visit int) (*visit) {
    if Visit <= visitsMaxCount {
        if visits1[Visit].Id == 0 {
            return nil
        }
        return &visits1[Visit]
    }

    return visits[Visit]
}

func getVisitSync(Visit int) (*visit) {
    if Visit <= visitsMaxCount {
        if visits1[Visit].Id == 0 {
            return nil
        }
        return &visits1[Visit]
    }

    visitsMutex.RLock()
    u := visits[Visit]
    visitsMutex.RUnlock()
    return u
}

func loadVisit(Visit int, v * visit_update) {
    vn := &visits1[Visit]

    vn.Id = Visit
    vn.Location = *v.Location
    vn.User = *v.User
    vn.Mark = *v.Mark
    vn.Visited_at = *v.Visited_at

    // Add to index
    User := vn.User
    Location := vn.Location

    u := getUser(User)
    l := getLocation(Location)

    z := usersVisits{Visit, l.Distance, l.CountryId, vn.Mark, l.PlaceId}
    u.Idx.Insert(vn.Visited_at, &z)

    iu := getIdxUserLoad(Location)
    iu.PushBack(&z)

    Age := (now - u.Birth_date) / (365.24 * 24 * 3600)
    z2 := locationsAvg{vn.Visited_at, Age, u.Gender, vn.Mark}
    l.Idx.Insert(Visit, &z2)

    il := getIdxLocationLoad(User)
    il.PushBack(&z2)
}

func insertVisit(Visit int, v * visit_update) {
    var vl * visit

    if Visit > visitsMaxCount {
        var vn visit
        vl = &vn

        visitsMutex.Lock()
        visits[Visit] = vl
        visitsMutex.Unlock()
    } else {
        vl = &visits1[Visit]
    }

    vl.Id = Visit
    vl.Location = *v.Location
    vl.User = *v.User
    vl.Mark = *v.Mark
    vl.Visited_at = *v.Visited_at
}
