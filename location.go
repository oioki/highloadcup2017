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

var locations map[int]*location
var locationsMutex sync.RWMutex

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
