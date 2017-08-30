package main

import (
    "log"
    "sync"
)

var _ = log.Println


type visit_update struct {
    Id          * int
    Location    * int
    User        * int
    Mark        * uint8
    Visited_at  * int
}

type visit struct {
    Id          int
    Location    int
    User        int
    Mark        uint8
    Visited_at  int
}

var visits map[int]*visit
var visitsMutex sync.RWMutex

const visitsMaxCount = 10000700
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

func getVisitInsert(Visit int) (*visit) {
    var v * visit

    if Visit > visitsMaxCount {
        var vn visit
        v = &vn

        visits[Visit] = v
    } else {
        v = &visits1[Visit]
    }

    return v
}

func getVisitInsertSync(Visit int) (*visit) {
    var v * visit

    if Visit > visitsMaxCount {
        var vn visit
        v = &vn

        visitsMutex.Lock()
        visits[Visit] = v
        visitsMutex.Unlock()
    } else {
        v = &visits1[Visit]
    }

    return v
}

func insertVisitData(v * visit, vu * visit_update) {
    Visit := *vu.Id

    v.Id = Visit
    v.Location = *vu.Location
    v.User = *vu.User
    v.Mark = *vu.Mark
    v.Visited_at = *vu.Visited_at

    // Add to index
    User := v.User
    Location := v.Location

    u := getUser(User)
    l := getLocation(Location)

    z := usersVisits{Visit, l.Distance, string(l.Country), v.Mark, l.Place}
    u.Idx.Insert(v.Visited_at, &z)
    l.Deps[&z] = true

    Age := (now - u.Birth_date) / (365.25 * 24 * 3600)
    z2 := locationsAvg{v.Visited_at, Age, u.Gender, int(v.Mark)}
    l.Idx.Insert(Visit, &z2)
    u.Deps[&z2] = true
}

func loadVisit(Visit int, vu * visit_update) {
    v := getVisitInsert(Visit)
    insertVisitData(v, vu)
}

func insertVisit(Visit int, vu * visit_update) {
    v := getVisitInsertSync(Visit)
    insertVisitData(v, vu)
}
