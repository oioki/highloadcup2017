package main

import (
//    "fmt"
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
}

var locations map[int]*location
var locationsMutex sync.RWMutex

const locationsMaxCount = 761314
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

func loadLocation(Location int, l * location_update) {
    ln := &locations1[Location]
    ln.Id = Location

    c, ok := placeId[*l.Place]
    if !ok {
        placeCount++
        placeId[*l.Place] = placeCount
        place[placeCount] = *l.Place
        c = placeCount
    }
    ln.PlaceId = c

    c, ok = countryId[*l.Country]
    if !ok {
        countryCount++
        countryId[*l.Country] = countryCount
        country[countryCount] = *l.Country
        c = countryCount
    }
    ln.CountryId = c

    c, ok = cityId[*l.City]
    if !ok {
        cityCount++
        cityId[*l.City] = cityCount
        city[cityCount] = *l.City
        c = cityCount
    }
    ln.CityId = c

    ln.Distance = *l.Distance
    ln.Idx = NewLocationsAvgIndex()
}

func insertLocation(Location int, l * location_update) {
    var ll * location

    if Location > locationsMaxCount {
        var ln location
        ll = &ln

        locationsMutex.Lock()
        locations[Location] = ll
        locationsMutex.Unlock()
    } else {
        ll = &locations1[Location]
    }

    ll.Id = Location

    // Note: assert that no new countries, cities or places
    ll.PlaceId = placeId[*l.Place]
    ll.CountryId = countryId[*l.Country]
    ll.CityId = cityId[*l.City]

    ll.Distance = *l.Distance
    ll.Idx = NewLocationsAvgIndex()
}
