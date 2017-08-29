package main

import (
    "log"
    "sync"
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
    PlaceId   int
    CountryId int
    CityId    int
    Distance  int

    Idx       LocationsAvgIndex
    Deps      map[*usersVisits]bool
}

var locations map[int]*location
var locationsMutex sync.RWMutex

const locationsMaxCount = 763407
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

    return locations[Location]
}

func getLocationSync(Location int) (*location) {
    if Location <= locationsMaxCount {
        if locations1[Location].Id == 0 {
            return nil
        }
        return &locations1[Location]
    }

    locationsMutex.RLock()
    l := locations[Location]
    locationsMutex.RUnlock()
    return l
}

func getLocationInsert(Location int) (*location) {
    var l * location

    if Location > locationsMaxCount {
        var ln location
        l = &ln

        locations[Location] = l
    } else {
        l = &locations1[Location]
    }

    return l
}

func getLocationInsertSync(Location int) (*location) {
    var l * location

    if Location > locationsMaxCount {
        var ln location
        l = &ln

        locationsMutex.Lock()
        locations[Location] = l
        locationsMutex.Unlock()
    } else {
        l = &locations1[Location]
    }

    return l
}

func insertLocationData(l * location, lu * location_update) {
    l.Id = *lu.Id

    c, ok := placeId[*lu.Place]
    if !ok {
        placeCount++
        placeId[*lu.Place] = placeCount
        place[placeCount] = *lu.Place
        c = placeCount
    }
    l.PlaceId = c

    c, ok = countryId[*lu.Country]
    if !ok {
        countryCount++
        countryId[*lu.Country] = countryCount
        country[countryCount] = *lu.Country
        c = countryCount
    }
    l.CountryId = c

    c, ok = cityId[*lu.City]
    if !ok {
        cityCount++
        cityId[*lu.City] = cityCount
        city[cityCount] = *lu.City
        c = cityCount
    }
    l.CityId = c

    l.Distance = *lu.Distance
    l.Idx = NewLocationsAvgIndex()
    l.Deps = make(map[*usersVisits]bool, 20)
}

func loadLocation(Location int, lu * location_update) {
    l := getLocationInsert(Location)
    insertLocationData(l, lu)
}

func insertLocation(Location int, lu * location_update) {
    l := getLocationInsertSync(Location)
    insertLocationData(l, lu)
}
