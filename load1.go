package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
)

func loadLocations1(filename string) {
    log.Println("loadLocations1", filename)

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

    var l location1

    for {
        dec.Decode(&l)

        l.Idx = NewLocationsAvgIndex()

        Location := l.Id
        locations1[Location] = l
        locationsCount++

        if !dec.More() {
            return
        }
    }
}

func loadUsers1(filename string) {
    log.Println("loadUsers1", filename)
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

    var u user1

    for {
        dec.Decode(&u)

        u.Idx = NewUsersVisitsIndex()

        User := u.Id
        users1[User] = u
        usersCount++

        if !dec.More() {
            return
        }
    }
}

func loadVisits1(filename string) {
    log.Println("loadVisits1", filename)

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

    var v visit1

    for {
        dec.Decode(&v)

        Visit := v.Id
        visits1[Visit] = v
        visitsCount++

        visitInsertHelper1(Visit)

        if !dec.More() {
            return
        }
    }
}

func loadAll1(root string) {
    files, err := ioutil.ReadDir(root)
    if err != nil {
        log.Fatal(err)
    }

    for _, file := range files {
        if file.Name()[0] == 108 {  // ord('l') = 108 = locations
            loadLocations1(root + "/" + file.Name())
        }
        if file.Name()[0] == 117 {  // ord('u') = 117 = users
            loadUsers1(root + "/" + file.Name())
        }
        if file.Name()[0] == 118 {  // ord('v') = 118 = visits
            loadVisits1(root + "/" + file.Name())
        }
    }

    log.Printf("Locations: %d / %d", locationsCount, locationsMaxCount)
    log.Printf("Users: %d / %d", usersCount, usersMaxCount)
    log.Printf("Visits: %d / %d", visitsCount, visitsMaxCount)
    log.Printf("IdxLocation: %d / %d", len(IdxLocation), locationsMaxCount)
    log.Printf("IdxUser: %d / %d", len(IdxUser), usersMaxCount)
}
