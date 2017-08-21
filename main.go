package main

// TODO: try to use gccgo

import (
    "fmt"
    "./slist"
    "errors"
    "github.com/valyala/fasthttp"
    "encoding/json"  // TODO: use instead https://github.com/buger/jsonparser
    "container/list"
    "io/ioutil"
    "log"
    "os"
    "strconv"
    "strings"
    "time"
)

type location struct {
    // native data
    Id        * int
    Place     * string
    Country   * string
    City      * string
    Distance  * int

    Raw         []byte

    // marks list
    Idx    * slist.BasicList
}

type user struct {
    Id          * int
    Email       * string
    First_name  * string
    Last_name   * string
    Gender      * string
    Birth_date  * int

    Raw           []byte

    // visits list
    Idx * slist.BasicList
}

type visit struct {
    Id          * int
    Location    * int
    User        * int
    Mark        * int
    Visited_at  * int

    Raw           []byte
}



type jsonLocationsType struct {
    Locations  []location
}

type jsonUsersType struct {
    Users  []user
}

type jsonVisitsType struct {
    Visits  []visit
}

var locations map[int]*location  // TODO: try to make map of location (not pointer)
var users map[int]*user
var visits map[int]*visit
var now int

// index used in users/:id/visits
var IdxUser map[int]*list.List

// index used in locations/:id/avg
var IdxLocation map[int]*list.List

func dumpPOST(ctx *fasthttp.RequestCtx) {
    log.Println(string(ctx.PostBody()))
}

func insertRawLocation(Location int, l * location) {
    locations[Location] = l
    l.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, *l.Place, *l.Country, *l.City, *l.Distance))
}

func updateRawLocation(Location int) {
    l := locations[Location]
    l.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"place\":\"%s\",\"country\":\"%s\",\"city\":\"%s\",\"distance\":%d}", Location, *l.Place, *l.Country, *l.City, *l.Distance))
}

func insertRawUser(User int, u * user) {
    users[User] = u
    u.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, *u.Email, *u.First_name, *u.Last_name, *u.Gender, *u.Birth_date))
}

func updateRawUser(User int) {
    u := users[User]
    u.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, *u.Email, *u.First_name, *u.Last_name, *u.Gender, *u.Birth_date))
}

func insertRawVisit(Visit int, v * visit) {
    visits[Visit] = v
    v.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"location\":%d,\"user\":%d,\"mark\":%d,\"visited_at\":%d}", Visit, *v.Location, *v.User, *v.Mark, *v.Visited_at))
}

func updateRawVisit(Visit int) {
    v := visits[Visit]
    v.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"location\":%d,\"user\":%d,\"mark\":%d,\"visited_at\":%d}", Visit, *v.Location, *v.User, *v.Mark, *v.Visited_at))
}


/*******************************************************************************
* Locations
*******************************************************************************/

func routineLocationUpdate(l location, ln * location, Location int) {
    updateIndexVisits := false
    if l.Place != nil {
        ln.Place = l.Place
        updateIndexVisits = true
    }
    if l.Country != nil {
        ln.Country = l.Country
        updateIndexVisits = true
    }
    if l.City != nil {
        ln.City = l.City
    }
    if l.Distance != nil {
        ln.Distance = l.Distance
        updateIndexVisits = true
    }

    if updateIndexVisits {
        l := ln
        if l != nil {
            // update all IdxUsers which depends on this Location
            UpdateIdxUser(Location, *l.Distance, l.Country, l.Place)
        } else {
            log.Println("locationUpdateHandler(): location not found", Location)  // unreachable code?
            return
        }
    }

    updateRawLocation(Location)
}

func locationUpdateHandler(ctx *fasthttp.RequestCtx, Location int) {
    //dumpPOST(ctx)

    var l location
    if unmarshal(ctx.PostBody(), &l) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    if ln, ok := locations[Location]; ok {
        go routineLocationUpdate(l, ln, Location)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func locationInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var l location
    if unmarshal(ctx.PostBody(), &l) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    incomplete_data :=
        l.Id == nil ||
        l.Place == nil ||
        l.Country == nil ||
        l.City == nil ||
        l.Distance == nil
    if incomplete_data {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    Location := *(l.Id)

    if _, ok := locations[Location]; ok {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertRawLocation(Location, &l)
        l.Idx = slist.NewBasicList()  // init avg index

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Users
*******************************************************************************/

func UpdateIdxLocation(User int, Age int, Gender * string) {
    if _, ok := IdxLocation[User]; !ok {
        //log.Printf("IdxLocation[User=%d] was not existed, now created. There were no visits of this user.", User)
        IdxLocation[User] = list.New()
    }
    IdxList := IdxLocation[User]

    for e := IdxList.Front(); e != nil; e = e.Next() {
        idx := e.Value.(*slist.Idx_locations_avg)

        idx.Age = Age
        idx.Gender = *Gender
    }
}

func UpdateIdxUser(Location int, Distance int, Country * string, Place * string) {
    if _, ok := IdxUser[Location]; !ok {
        //log.Printf("IdxUser[Location=%d] was not existed, now created. There were no visits to this location.", Location)
        IdxUser[Location] = list.New()
    }
    IdxList := IdxUser[Location]

    for e := IdxList.Front(); e != nil; e = e.Next() {
        idx := e.Value.(*slist.Idx_users_visits)

        idx.Distance = Distance
        idx.Country = *Country
        idx.Place = *Place

        idx.Raw = []byte(fmt.Sprintf("{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", idx.Mark, idx.Visited_at, idx.Place))
    }
}

func routineUserUpdate(u user, un * user, User int) {
    updateIndexAvg := false
    if u.Email != nil {
        un.Email = u.Email
    }
    if u.First_name != nil {
        un.First_name = u.First_name
    }
    if u.Last_name != nil {
        un.Last_name = u.Last_name
    }
    if u.Gender != nil {
        un.Gender = u.Gender
        updateIndexAvg = true
    }
    if u.Birth_date != nil {
        un.Birth_date = u.Birth_date
        updateIndexAvg = true
    }

    if updateIndexAvg {
        u := un
        if u != nil {
            Age := (now - *u.Birth_date) / (365.24 * 24 * 3600)
            UpdateIdxLocation(User, Age, u.Gender)
        } else {
            log.Println("userUpdateHandler(): user not found", User)  // unreachable code?
            return
        }
    }

    updateRawUser(User)
}

func userUpdateHandler(ctx *fasthttp.RequestCtx, User int) {
    //dumpPOST(ctx)

    var u user

    if unmarshal(ctx.PostBody(), &u) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    if un, ok := users[User]; ok {
        go routineUserUpdate(u, un, User)
        ctx.Write([]byte("{}"))

    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func unmarshal(body []byte, value interface{}) (error) {
    // https://gist.github.com/aodin/9493190

    // unmarshal
    err := json.Unmarshal(body, &value)
    if err != nil {
        return err
    }

    // check for 'null'
    // https://golang.org/pkg/encoding/json/#Unmarshal
    // The JSON null value unmarshals into an interface, map, pointer, or slice
    // by setting that Go value to nil. Because null is often used in JSON to mean
    // “not present,” unmarshaling a JSON null into any other Go type has no effect
    // on the value and produces no error.
    if strings.Contains(string(body), ": null") {  // TODO
        return errors.New("null found")
    }

    return nil
}

func userInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var u user
    if unmarshal(ctx.PostBody(), &u) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    incomplete_data :=
        u.Id == nil ||
        u.Email == nil ||
        u.First_name == nil ||
        u.Last_name == nil ||
        u.Gender == nil ||
        u.Birth_date == nil
    if incomplete_data {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    User := *(u.Id)

    if _, ok := users[User]; ok {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertRawUser(User, &u)
        u.Idx = slist.NewBasicList()  // init visits index

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Visits
*******************************************************************************/

func routineVisitUpdate(vi visit, vn * visit, Visit int) {
    old_location := *vn.Location
    old_user := *vn.User
    if vi.Location != nil {
        vn.Location = vi.Location
    }
    if vi.User != nil {
        vn.User = vi.User
    }
    if vi.Mark != nil {
        vn.Mark = vi.Mark
    }
    if vi.Visited_at != nil {
        vn.Visited_at = vi.Visited_at
    }

    v := vn
    Location := *v.Location
    User := *v.User
    l := locations[Location]
    u := users[User]

    // temporary item for Idx_locations_avg
    Age := (now - *u.Birth_date) / (365.24 * 24 * 3600)
    newIdxLocations := slist.Idx_locations_avg{*v.Visited_at, Age, *u.Gender, *v.Mark}

    // temporary item for Idx_users_visits
    newIdxUsersVisits := slist.Idx_users_visits{*v.Visited_at, Visit, *l.Distance, *l.Country, *v.Mark, *l.Place, []byte(fmt.Sprintf("{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", *v.Mark, *v.Visited_at, *l.Place))}

    var idxLocationsRemoved *slist.Idx_locations_avg
    var idxVisitsRemoved *slist.Idx_users_visits
    var err error

    var lr *location
    // update index /locations/:id/avg
    if old_location != Location {
        lr = locations[old_location]
    } else {
        lr = l
    }

    if lr != nil {  // if old location existed
        //log.Printf("deleting item (%d) from locations[remove_location=%d] index (LocationAvg)", Visit, remove_location)
        idxLocationsRemoved, err = lr.Idx.Remove(Visit)
        _ = err  // TODO: error check?
    }


    var ur *user
    // update index /users/:id/visits
    if old_user != User {
        ur = users[old_user]
    } else {
        ur = u
    }

    if ur != nil {  // if old user existed
        //log.Printf("deleting item (%d) from users[remove_user=%d] index (UsersVisits)", User, remove_user)
        idxVisitsRemoved, err = ur.Idx.RemoveByVisit(Visit)
        _ = err  // TODO: error check?
    }

    // remove this index from dependency list of IdxUser[old_location]
    if old_location != Location {
        for e := IdxUser[old_location].Front(); e != nil; e = e.Next() {
            if e.Value == idxVisitsRemoved {
                IdxUser[old_location].Remove(e)
                break
            }
        }
    }

    // remove this index from dependency list of IdxLocation[old_user]
    if old_user != User {
        for e := IdxLocation[old_user].Front(); e != nil; e = e.Next() {
            if e.Value == idxLocationsRemoved {
                IdxLocation[old_user].Remove(e)
                break
            }
        }
    }

    l.Idx.Insert(Visit, &newIdxLocations)  // add it to new_location
    if _, ok := IdxLocation[User]; !ok {
        IdxLocation[User] = list.New()
    }
    IdxLocation[User].PushBack(&newIdxLocations)

    u.Idx.Insert(*v.Visited_at, &newIdxUsersVisits)  // add it to new_user
    if _, ok := IdxUser[Location]; !ok {
        IdxUser[Location] = list.New()
    }
    IdxUser[Location].PushBack(&newIdxUsersVisits)

    updateRawVisit(Visit)
}

func visitUpdateHandler(ctx *fasthttp.RequestCtx, Visit int) {
    //dumpPOST(ctx)

    var v visit
    if unmarshal(ctx.PostBody(), &v) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    if vn, ok := visits[Visit]; ok {
        go routineVisitUpdate(v, vn, Visit)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func visitInsertHelper(Visit int, v * visit) {
    insertRawVisit(Visit, v)

    // Add to index
    User := *v.User
    Location := *v.Location

    // TODO: check if user and location exists
    u := users[User]
    l := locations[Location]

    z := slist.Idx_users_visits{*v.Visited_at, Visit, *l.Distance, *l.Country, *v.Mark, *l.Place, []byte(fmt.Sprintf("{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", *v.Mark, *v.Visited_at, *l.Place))}
    u.Idx.Insert(*v.Visited_at, &z)

    if _, ok := IdxUser[Location]; !ok {
        IdxUser[Location] = list.New()
    }
    IdxUser[Location].PushBack(&z)



    Age := (now - *u.Birth_date) / (365.24 * 24 * 3600)
    z2 := slist.Idx_locations_avg{*v.Visited_at, Age, *u.Gender, *v.Mark}
    l.Idx.Insert(Visit, &z2)

    if _, ok := IdxLocation[User]; !ok {
        IdxLocation[User] = list.New()
    }
    IdxLocation[User].PushBack(&z2)
}

func visitInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var v visit
    if unmarshal(ctx.PostBody(), &v) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    incomplete_data :=
        v.Id == nil ||
        v.Location == nil ||
        v.User == nil ||
        v.Mark == nil ||
        v.Visited_at == nil
    if incomplete_data {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    Visit := *(v.Id)

    if _, ok := visits[Visit]; ok {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go visitInsertHelper(Visit, &v)
        ctx.Write([]byte("{}"))
    }
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
         l.Idx = slist.NewBasicList()  // init avg index
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
         u.Idx = slist.NewBasicList()  // init visits index
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

func locationAvgHandler(ctx *fasthttp.RequestCtx, Location int) {
    // Parse GET parameters
    qa := ctx.URI().QueryArgs()
    fromDateStr := string(qa.Peek("fromDate"))
    toDateStr := string(qa.Peek("toDate"))
    fromAgeStr := string(qa.Peek("fromAge"))
    toAgeStr := string(qa.Peek("toAge"))
    gender := string(qa.Peek("gender"))

    skipFromDate, skipToDate, skipFromAge, skipToAge, skipGender := true, true, true, true, true
    fromDate, toDate, fromAge, toAge := 0,0,0,0

    var err error
    if (len(fromDateStr) != 0) {
        skipFromDate = false
        fromDate, err = strconv.Atoi(fromDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (toDateStr != "") {
        skipToDate = false
        toDate, err = strconv.Atoi(toDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (fromAgeStr != "") {
        skipFromAge = false
        fromAge, err = strconv.Atoi(fromAgeStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (toAgeStr != "") {
        skipToAge = false
        toAge, err = strconv.Atoi(toAgeStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (gender != "") {
        skipGender = false
        if ! ( (gender=="f") || (gender=="m")) {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    //log.Println(Location, fromDateStr, toDateStr, fromAgeStr, toAgeStr, gender);

    // calc 'avg' for location 'Location'
    if l, ok := locations[Location]; ok {
        avg := l.Idx.CalcAvg(skipFromDate, skipToDate, skipFromAge, skipToAge, skipGender, fromDate, toDate, fromAge, toAge, gender)
        ctx.Write([]byte("{\"avg\":"))
        ctx.Write([]byte(avg))
        ctx.Write([]byte("}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func usersVisitsHandler(ctx *fasthttp.RequestCtx, User int) {
    //start := time.Now() ; last := start

    // Parse GET parameters
    qa := ctx.URI().QueryArgs()
    fromDateStr := string(qa.Peek("fromDate"))
    toDateStr := string(qa.Peek("toDate"))
    country := string(qa.Peek("country"))
    toDistanceStr := string(qa.Peek("toDistance"))

    //log.Printf("%10s r.URL.Query()\n", time.Since(last)) ; last = time.Now()

    skipFromDate, skipToDate, skipCountry, skipToDistance := true, true, true, true
    fromDate, toDate, toDistance := 0,0,0

    var err error
    if (fromDateStr != "") {
        skipFromDate = false
        fromDate, err = strconv.Atoi(fromDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (toDateStr != "") {
        skipToDate = false
        toDate, err = strconv.Atoi(toDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (toDistanceStr != "") {
        skipToDistance = false
        toDistance, err = strconv.Atoi(toDistanceStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (country != "") {
        skipCountry = false
    }

    if u, ok := users[User]; ok {
        // 20-30 microseconds
        u.Idx.VisitsHandler(ctx, skipFromDate, skipToDate, skipCountry, skipToDistance, fromDate, toDate, country, toDistance)
        //log.Printf("%10s VisitsHandler\n", time.Since(last)) ; last = time.Now()
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func router(ctx *fasthttp.RequestCtx) {
    method, uri := ctx.Method(), ctx.Path()
    ctx.SetConnectionClose()

    lu := len(uri)

    // We should check for '/' request, but skip for now
    //if lu < 2 {
    //    ctx.SetStatusCode(fasthttp.StatusNotFound)
    //    return
    //}

    method_char, uri_char := method[0], uri[1]
    switch method_char {
        case 71:  // = ord('G') = GET
            switch uri_char {
            case 108:  // = ord('l') = /locations
                last_char := uri[lu-1]
                switch last_char {
                case 103:  // = ord('g') => /locations/:id/avg
                    id, err := strconv.Atoi(string(uri[11:lu-4]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/locations/:id/avg", id)
                        locationAvgHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                default:
                    id, err := strconv.Atoi(string(uri[11:lu]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/locations/:id", id)
                        //locationSelectHandler(ctx, id)
                        if l, ok := locations[id]; ok {
                            ctx.Write(l.Raw)
                        } else {
                            ctx.SetStatusCode(fasthttp.StatusNotFound)
                        }
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                }

            case 117:  // = ord('u') = /users
                // len('/users/100124') == 13
                // len('/users/1/visits') == 15
                // Therefore, we can distinguish /users/:id and /users/:id/visits just by length of URI
                if lu > 13 {  // GET /users/:id/visits
                    id, err := strconv.Atoi(string(uri[7:lu-7]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id/visits", id)
                        usersVisitsHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                } else {  // GET /users/:id
                    id, err := strconv.Atoi(string(uri[7:lu]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id", id)
                        //userSelectHandler(ctx, id)
                        if u, ok := users[id]; ok {
                            ctx.Write(u.Raw)
                        } else {
                            ctx.SetStatusCode(fasthttp.StatusNotFound)
                        }
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusNotFound)  // holywar fix instead of 400
                    }
                }

            case 118:  // = ord('v') = /visits
                id, err := strconv.Atoi(string(uri[8:lu]))
                if err == nil {
                    //log.Printf("%s %q %s %d", method, uri, "/visits/:id", id)
                    //visitSelectHandler(ctx, id)
                    if v, ok := visits[id]; ok {
                        ctx.Write(v.Raw)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusNotFound)
                    }
                } else {
                    ctx.SetStatusCode(fasthttp.StatusBadRequest)
                }

            default:
                ctx.SetStatusCode(fasthttp.StatusBadRequest)
            }

        case 80:  // = ord('P') = POST
            switch uri_char {
            case 108:  // = ord('l') = /locations
                last_char := uri[lu-1]
                switch last_char {
                case 119:  // = ord('w') => /locations/new
                    //log.Printf("%s %q %s", method, uri, "/locations/new")
                    locationInsertHandler(ctx)
                default:
                    id, err := strconv.Atoi(string(uri[11:lu]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/locations/:id", id)
                        locationUpdateHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                }

            case 117:  // = ord('u') = /users
                last_char := uri[lu-1]
                switch last_char {
                case 119:  // = ord('w') => /users/new
                    //log.Printf("%s %q %s", method, uri, "/users/new")
                    //log.Println("POST", string(ctx.PostBody()))
                    userInsertHandler(ctx)
                default:
                    id, err := strconv.Atoi(string(uri[7:lu]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id", id)
                        userUpdateHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                }

            case 118:  // = ord('v') = /visits
                last_char := uri[lu-1]
                switch last_char {
                case 119:  // = ord('w') => /visits/new
                    //log.Printf("%s %q %s", method, uri, "/visits/new")
                    visitInsertHandler(ctx)
                default:
                    id, err := strconv.Atoi(string(uri[8:lu]))
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/visits/:id", id)
                        visitUpdateHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                }

            default:
                ctx.SetStatusCode(fasthttp.StatusNotFound)
            }

        default:
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
    }
}

func main () {
    log.Println("HighLoad Cup 2017 solution 17 by oioki")

    now = int(time.Now().Unix())

    // Create shared data structures
    locations = make(map[int]*location)
    users = make(map[int]*user)
    visits = make(map[int]*visit)

    IdxUser = make(map[int]*list.List)
    IdxLocation = make(map[int]*list.List)

    // Read input files
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
    log.Println("You're ready, go!")

    // list of indexes 'Location' -> index
    // index itself is mapping 'Visited_at' -> UsersVisits[Visit, Distance, Country, Mark, Place]
    // this index is used in request /users/:id/visits
    // 800 is Location
    //for e := IdxUser[116].Front(); e != nil; e = e.Next() {
    //    fmt.Println(e.Value)
    //}

    // list of indexes 'User' -> index
    // index itself is mapping 'Location' -> LocationAvg[Visited_at, Birth_date, Gender, Mark]
    // this index is used in request /locations/:id/avg
    // 800 is User
    //for e := IdxLocation[900].Front(); e != nil; e = e.Next() {
    //    fmt.Println(e.Value)
    //}

    fasthttp.ListenAndServe(":80", router)
}
