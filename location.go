package main

import (
    "fmt"
    "sync"
)

type location struct {
    Id        * int
    Place     * string
    Country   * string
    City      * string
    Distance  * int

    Raw         []byte

    Idx       LocationsAvgIndex
}

type location1 struct {
    Id        int
    Place     string
    Country   string
    City      string
    Distance  int

    Raw       []byte

    Idx       LocationsAvgIndex
}

var locations map[int]*location
var locationsMutex sync.RWMutex

const locationsMaxCount = 761314
var locationsCount int
var locations1[locationsMaxCount+1]location1

func getLocation(Location int) (*location, bool) {
    locationsMutex.RLock()
    l, err := locations[Location]
    locationsMutex.RUnlock()
    return l, err
}

func insertRawLocation(Location int, l * location) {
    locationsMutex.Lock()
    locations[Location] = l
    locationsMutex.Unlock()
    l.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, *l.Place, *l.Country, *l.City, *l.Distance))
}

func updateRawLocation(Location int, l * location) {
    l.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, *l.Place, *l.Country, *l.City, *l.Distance))
}

func updateRawLocation1(Location int) {
    locations1[Location].Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, locations1[Location].Place, locations1[Location].Country, locations1[Location].City, locations1[Location].Distance))
}
