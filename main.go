package main

import (
    "fmt"
    "errors"
    "github.com/valyala/fasthttp"
    "encoding/json"
    "container/list"
    "log"
    "strings"
    "time"
)

var now int

func dumpPOST(ctx *fasthttp.RequestCtx) {
    log.Println(string(ctx.PostBody()))
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

        // update all IdxUsers which depends on this Location
        UpdateIdxUser(Location, *l.Distance, l.Country, l.Place)
    }

    updateRawLocation(Location, ln)
}

func locationUpdateHandler(ctx *fasthttp.RequestCtx, Location int) {
    //dumpPOST(ctx)

    var l location
    if unmarshal(ctx.PostBody(), &l) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    if ln, ok := getLocation(Location); ok {
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

    if _, ok := getLocation(Location); ok {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertRawLocation(Location, &l)
        l.Idx = NewLocationsAvgIndex()

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Users
*******************************************************************************/

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

        Age := (now - *u.Birth_date) / (365.24 * 24 * 3600)
        UpdateIdxLocation(User, Age, u.Gender)
    }

    updateRawUser(User, un)
}

func userUpdateHandler(ctx *fasthttp.RequestCtx, User int) {
    //dumpPOST(ctx)

    var u user

    if unmarshal(ctx.PostBody(), &u) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    if un, ok := getUser(User); ok {
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
    if strings.Contains(string(body), ": null") {
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

    if _, ok := getUser(User); ok {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertRawUser(User, &u)
        u.Idx = NewUsersVisitsIndex()

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
    l, _ := getLocation(Location)
    u, _ := getUser(User)

    // temporary item for locationsAvg
    Age := (now - *u.Birth_date) / (365.24 * 24 * 3600)
    newIdxLocations := locationsAvg{*v.Visited_at, Age, *u.Gender, *v.Mark}

    // temporary item for usersVisits
    newIdxUsersVisits := usersVisits{*v.Visited_at, Visit, *l.Distance, *l.Country, *v.Mark, *l.Place, []byte(fmt.Sprintf("{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", *v.Mark, *v.Visited_at, *l.Place))}

    var idxLocationsRemoved *locationsAvg
    var idxVisitsRemoved *usersVisits

    var lr *location
    // update index /locations/:id/avg
    if old_location != Location {
        lr, _ = getLocation(old_location)
    } else {
        lr = l
    }

    if lr != nil {  // if old location existed
        //log.Printf("deleting item (%d) from locations[remove_location=%d] index (LocationAvg)", Visit, remove_location)
        idxLocationsRemoved = lr.Idx.Remove(Visit)
    }


    var ur *user
    // update index /users/:id/visits
    if old_user != User {
        ur, _ = getUser(old_user)
    } else {
        ur = u
    }

    if ur != nil {  // if old user existed
        //log.Printf("deleting item (%d) from users[remove_user=%d] index (UsersVisits)", User, remove_user)
        idxVisitsRemoved = ur.Idx.RemoveByVisit(Visit)
    }

    // remove this index from dependency list of IdxUser[old_location]
    if old_location != Location {
        iu := getIdxUser(old_location)
        for e := iu.Front(); e != nil; e = e.Next() {
            if e.Value == idxVisitsRemoved {
                iu.Remove(e)
                break
            }
        }
    }

    // remove this index from dependency list of IdxLocation[old_user]
    if old_user != User {
        il := getIdxLocation(old_user)
        for e := il.Front(); e != nil; e = e.Next() {
            if e.Value == idxLocationsRemoved {
                il.Remove(e)
                break
            }
        }
    }

    l.Idx.Insert(Visit, &newIdxLocations)  // add it to new_location
    il := getIdxLocation(User)
    il.PushBack(&newIdxLocations)

    u.Idx.Insert(*v.Visited_at, &newIdxUsersVisits)  // add it to new_user
    iu := getIdxUser(Location)
    iu.PushBack(&newIdxUsersVisits)

    updateRawVisit(Visit, vn)
}

func visitUpdateHandler(ctx *fasthttp.RequestCtx, Visit int) {
    //dumpPOST(ctx)

    var v visit
    if unmarshal(ctx.PostBody(), &v) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    if vn, ok := getVisit(Visit); ok {
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

    u, _ := getUser(User)
    l, _ := getLocation(Location)

    z := usersVisits{*v.Visited_at, Visit, *l.Distance, *l.Country, *v.Mark, *l.Place, []byte(fmt.Sprintf("{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", *v.Mark, *v.Visited_at, *l.Place))}
    u.Idx.Insert(*v.Visited_at, &z)

    iu := getIdxUser(Location)
    iu.PushBack(&z)


    Age := (now - *u.Birth_date) / (365.24 * 24 * 3600)
    z2 := locationsAvg{*v.Visited_at, Age, *u.Gender, *v.Mark}
    l.Idx.Insert(Visit, &z2)

    il := getIdxLocation(User)
    il.PushBack(&z2)
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

    if _, ok := getVisit(Visit); ok {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go visitInsertHelper(Visit, &v)
        ctx.Write([]byte("{}"))
    }
}


func locationAvgHandler(ctx *fasthttp.RequestCtx, Location int) {
    // Parse GET parameters
    qa := ctx.URI().QueryArgs()
    fromDateStr := qa.Peek("fromDate")
    toDateStr := qa.Peek("toDate")
    fromAgeStr := qa.Peek("fromAge")
    toAgeStr := qa.Peek("toAge")
    gender := string(qa.Peek("gender"))

    skipGender := true
    fromDate, toDate, fromAge, toAge := 0,4294967295,0,4294967295

    var err error
    if (len(fromDateStr) > 0) {
        fromDate, err = Atoi(fromDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (len(toDateStr) > 0) {
        toDate, err = Atoi(toDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (len(fromAgeStr) > 0) {
        fromAge, err = Atoi(fromAgeStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (len(toAgeStr) > 0) {
        toAge, err = Atoi(toAgeStr)
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

    // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
    if l, ok := locations[Location]; ok {
        l.Idx.CalcAvg(ctx, skipGender, fromDate, toDate, fromAge, toAge, gender)
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func usersVisitsHandler(ctx *fasthttp.RequestCtx, User int) {
    //start := time.Now() ; last := start

    // Parse GET parameters
    qa := ctx.URI().QueryArgs()
    fromDateStr := qa.Peek("fromDate")
    toDateStr := qa.Peek("toDate")
    country := string(qa.Peek("country"))
    toDistanceStr := qa.Peek("toDistance")

    //log.Printf("%10s r.URL.Query()\n", time.Since(last)) ; last = time.Now()

    skipCountry := true
    fromDate, toDate, toDistance := 0,4294967295,4294967295

    var err error
    if (len(fromDateStr) > 0) {
        fromDate, err = Atoi(fromDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (len(toDateStr) > 0) {
        toDate, err = Atoi(toDateStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (len(toDistanceStr) > 0) {
        toDistance, err = Atoi(toDistanceStr)
        if err != nil {
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    if (country != "") {
        skipCountry = false
    }

    // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
    if u, ok := users[User]; ok {
        u.Idx.VisitsHandler(ctx, skipCountry, fromDate, toDate, country, toDistance)
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func router(ctx *fasthttp.RequestCtx) {
    method, uri := ctx.Method(), ctx.Path()

    // We will set Connection header in fasthttp code
    //ctx.Response.Header.Set("Connection", "keep-alive")

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
                    id, err := Atoi(uri[11:lu-4])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/locations/:id/avg", id)
                        locationAvgHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                default:
                    id, err := Atoi(uri[11:lu])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/locations/:id", id)
                        //locationSelectHandler(ctx, id)
                        // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
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
                    id, err := Atoi(uri[7:lu-7])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id/visits", id)
                        usersVisitsHandler(ctx, id)
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                } else {  // GET /users/:id
                    id, err := Atoi(uri[7:lu])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id", id)
                        //userSelectHandler(ctx, id)
                        // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
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
                id, err := Atoi(uri[8:lu])
                if err == nil {
                    //log.Printf("%s %q %s %d", method, uri, "/visits/:id", id)
                    //visitSelectHandler(ctx, id)
                    // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
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
                    id, err := Atoi(uri[11:lu])
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
                    id, err := Atoi(uri[7:lu])
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
                    id, err := Atoi(uri[8:lu])
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

var req    * fasthttp.Request
var resp   * fasthttp.Response
var client * fasthttp.Client

func warmup(url string) {
    req.SetRequestURI(url)
    client.Do(req, resp)
    _ = resp.Body()
}

func warmupAll() {
    time.Sleep(1000 * time.Millisecond)

    req = fasthttp.AcquireRequest()
    resp = fasthttp.AcquireResponse()
    client = &fasthttp.Client{}

    for k, _ := range locations {
        warmup(fmt.Sprintf("http://127.0.0.1/locations/%d", k))
        warmup(fmt.Sprintf("http://127.0.0.1/locations/%d/avg", k))
    }
    log.Println("/locations/:id{,/avg} warmup done")

    for k, _ := range users {
        warmup(fmt.Sprintf("http://127.0.0.1/users/%d", k))
        warmup(fmt.Sprintf("http://127.0.0.1/users/%d/visits", k))
    }
    log.Println("/users/:id{,/visits} warmup done")

    for k, _ := range visits {
        warmup(fmt.Sprintf("http://127.0.0.1/visits/%d", k))
    }
    log.Println("/visits/:id warmup done")
}

func main () {
    log.Println("HighLoad Cup 2017 solution 32 by oioki")

    now = int(time.Now().Unix())

    // Create shared data structures
    locations = make(map[int]*location)
    users = make(map[int]*user)
    visits = make(map[int]*visit)

    IdxUser = make(map[int]*list.List)
    IdxLocation = make(map[int]*list.List)

    loadAll()

    log.Println("You're ready, go!")

    go warmupAll()

    fasthttp.ListenAndServe(":80", router)
}
