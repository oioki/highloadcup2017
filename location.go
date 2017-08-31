package main

import (
    "log"
)

var _ = log.Println


type location_update struct {
    Id        * int
    Place     * string
    Country   * string
    City      * string
    Distance  * int
}

type location struct {
    Id        int
    Place     []byte
    Country   []byte
    City      []byte
    Distance  int

    Idx       LocationsAvgIndex
    Deps      map[*usersVisits]bool
}

const locationsMaxCount = 763407 + 7000
var locationsCount int
var locations1[locationsMaxCount+1]location

// Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
func getLocation(Location int) (*location) {
    if Location <= locationsMaxCount {
        if locations1[Location].Id == 0 {
            return nil
        }
        return &locations1[Location]
    }

    return nil
}

func getLocationInsert(Location int) (*location) {
    return &locations1[Location]
}

func insertLocationData(l * location, lu * location_update) {
    l.Id = *lu.Id
    l.Place = []byte(*lu.Place)
    l.Country = []byte(*lu.Country)
    l.City = []byte(*lu.City)
    l.Distance = *lu.Distance
    l.Idx = NewLocationsAvgIndex()
    l.Deps = make(map[*usersVisits]bool, 20)
}

func loadLocation(Location int, lu * location_update) {
    l := getLocationInsert(Location)
    insertLocationData(l, lu)
}

func insertLocation(Location int, lu * location_update) {
    l := getLocationInsert(Location)
    insertLocationData(l, lu)
}
