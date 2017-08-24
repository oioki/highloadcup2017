package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
)

type jsonLocationsType struct {
    Locations  []location
}

type jsonUsersType struct {
    Users  []user
}

type jsonVisitsType struct {
    Visits  []visit
}

func loadLocations(filename string) {
    //start := time.Now()

    file, e := ioutil.ReadFile(filename)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    }

    var jsonLocations jsonLocationsType
    json.Unmarshal(file, &jsonLocations)

    for i := range jsonLocations.Locations {
        l := jsonLocations.Locations[i]
        Location := l.Id
        l.Idx = NewLocationsAvgIndex()
        insertRawLocation(*Location, &l)
    }

    //log.Printf("loadLocations %s: %d, %s", filename, len(jsonLocations.Locations), time.Since(start))
}

func loadUsers(filename string) {
    //start := time.Now()

    file, e := ioutil.ReadFile(filename)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    }

    var jsonUsers jsonUsersType
    json.Unmarshal(file, &jsonUsers)

    for i := range jsonUsers.Users {
        u := jsonUsers.Users[i]
        User := u.Id
        u.Idx = NewUsersVisitsIndex()
        insertRawUser(*User, &u)
    }

    //log.Printf("loadUsers %s: %d, %s", filename, len(jsonUsers.Users), time.Since(start))
}

func loadVisits(filename string) {
    //start := time.Now()

    file, e := ioutil.ReadFile(filename)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    }

    var jsonVisits jsonVisitsType
    json.Unmarshal(file, &jsonVisits)

    for i := range jsonVisits.Visits {
        v := jsonVisits.Visits[i]
        Visit := *(v.Id)
        visitInsertHelper(Visit, &v)
    }

    //log.Printf("loadVisits %s: %d, %s", filename, len(jsonVisits.Visits), time.Since(start))
}

func loadAll() {
    files, err := ioutil.ReadDir("/root")
    if err != nil {
        log.Fatal(err)
    }

    for _, file := range files {
        if file.Name()[0] == 108 {  // ord('l') = 108 = locations
            loadLocations("/root/" + file.Name())
        }
        if file.Name()[0] == 117 {  // ord('u') = 117 = users
            loadUsers("/root/" + file.Name())
        }
        if file.Name()[0] == 118 {  // ord('v') = 118 = visits
            loadVisits("/root/" + file.Name())
        }
    }
}
