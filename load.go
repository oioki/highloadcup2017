package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "runtime"
)

var _ = runtime.GC

func loadLocations(filename string) {
    log.Println("loadLocations", filename)

    file, e := os.Open(filename)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    }
    defer file.Close()

    dec := json.NewDecoder(file)

    // skip  '{', 'locations', '['
    _,_ = dec.Token()
    _,_ = dec.Token()
    _,_ = dec.Token()

    var l location_update
    for {
        dec.Decode(&l)
        loadLocation(*l.Id, &l)
        locationsCount++

        if !dec.More() {
            return
        }
    }
}

func loadUsers(filename string) {
    log.Println("loadUsers", filename)

    file, e := os.Open(filename)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    }
    defer file.Close()

    dec := json.NewDecoder(file)

    // skip  '{', 'users', '['
    _,_ = dec.Token()
    _,_ = dec.Token()
    _,_ = dec.Token()

    var u user_update
    for {
        dec.Decode(&u)
        loadUser(*u.Id, &u)
        usersCount++

        if !dec.More() {
            return
        }
    }
}

func loadVisits(filename string) {
    log.Println("loadVisits", filename)

    file, e := os.Open(filename)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    }
    defer file.Close()

    dec := json.NewDecoder(file)

    // skip  '{', 'visits', '['
    _,_ = dec.Token()
    _,_ = dec.Token()
    _,_ = dec.Token()

    var v visit_update
    for {
        dec.Decode(&v)
        loadVisit(*v.Id, &v)
        visitsCount++

        if !dec.More() {
            return
        }
    }
}

func loadAll(root string) {
    files, err := ioutil.ReadDir(root)
    if err != nil {
        log.Fatal(err)
    }

    for _, file := range files {
        if file.Name()[0] == 108 {  // ord('l') = 108 = locations
            loadLocations(root + "/" + file.Name())
        }
        if file.Name()[0] == 117 {  // ord('u') = 117 = users
            loadUsers(root + "/" + file.Name())
        }
        if file.Name()[0] == 118 {  // ord('v') = 118 = visits
            loadVisits(root + "/" + file.Name())
        }
    }

    //runtime.GC()

    log.Printf("Locations: %d + %d = %d", locationsMaxCount, len(locations), locationsCount)
    log.Printf("Users: %d + %d = %d", usersMaxCount, len(users), usersCount)
    log.Printf("Visits: %d + %d = %d", visitsMaxCount, len(visits), visitsCount)
    log.Printf("Countries: %d / %d", countryCount, countryMaxCount)
    log.Printf("Cities: %d / %d", cityCount, cityMaxCount)
    log.Printf("Places: %d / %d", placeCount, placeMaxCount)
}
