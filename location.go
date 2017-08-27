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

    //Raw         []byte

    Idx       LocationsAvgIndex
}

type location1 struct {
    Id        int
    Place     string
    Country   string
    City      string
    Distance  int

    //Raw       []byte

    Idx       LocationsAvgIndex
}

var locations map[int]*location
var locationsMutex sync.RWMutex

const locationsMaxCount = 761314+40000
var locationsCount int
//var locations1[locationsMaxCount+1]location1
var locations1[1]location1

func getLocation(Location int) (*location, bool) {
    locationsMutex.RLock()
    l, err := locations[Location]
    locationsMutex.RUnlock()
    return l, err
}

func insertRawLocationLoad(Location int, l * location_update) {
    var ln location
    locations[Location] = &ln
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

func insertRawLocation(Location int, l * location_update) {
    locationsMutex.Lock()
    var ln location
    locations[Location] = &ln
    ln.Id = Location

    // Note: assert that no new countries, cities or places
    ln.PlaceId = placeId[*l.Place]
    ln.CountryId = countryId[*l.Country]
    ln.CityId = cityId[*l.City]

    ln.Distance = *l.Distance
    ln.Idx = NewLocationsAvgIndex()
    locationsMutex.Unlock()
//    l.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, *l.Place, *l.Country, *l.City, *l.Distance))
}

func updateRawLocation(Location int, l * location) {
//    l.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, *l.Place, *l.Country, *l.City, *l.Distance))
}

func updateRawLocation1(Location int) {
//    locations1[Location].Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, locations1[Location].Place, locations1[Location].Country, locations1[Location].City, locations1[Location].Distance))
}
