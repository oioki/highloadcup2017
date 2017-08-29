package main

import (
    "github.com/valyala/fasthttp"
    "container/list"
    "log"
    "os"
    "time"
)

var now int

func dumpPOST(ctx *fasthttp.RequestCtx) {
    log.Println(string(ctx.PostBody()))
}


/*******************************************************************************
* Locations
*******************************************************************************/

func routineLocationUpdate(l location_update, ln * location, Location int) {
    updateIndexVisits := false
    if l.Place != nil {
        ln.PlaceId = placeId[*l.Place]
        updateIndexVisits = true
    }
    if l.Country != nil {
        ln.CountryId = countryId[*l.Country]
        updateIndexVisits = true
    }
    if l.City != nil {
        ln.CityId = cityId[*l.City]
    }
    if l.Distance != nil {
        ln.Distance = *l.Distance
        updateIndexVisits = true
    }

    if updateIndexVisits {
        l := ln

        // update all IdxUsers which depends on this Location
        UpdateIdxUser(Location, l.Distance, l.CountryId, l.PlaceId)
    }
}

func locationUpdateHandler(ctx *fasthttp.RequestCtx, Location int) {
    //dumpPOST(ctx)

    var l location_update
    if unmarshal(ctx.PostBody(), &l) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    ln := getLocationSync(Location)
    if ln != nil {
        go routineLocationUpdate(l, ln, Location)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func locationInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var l location_update
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

    if getLocationSync(Location) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertLocation(Location, &l)

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Users
*******************************************************************************/

func routineUserUpdate(u user_update, un * user, User int) {
    updateIndexAvg := false
    if u.Email != nil {
        un.Email = *u.Email
    }
    if u.First_name != nil {
        un.First_name = *u.First_name
    }
    if u.Last_name != nil {
        un.Last_name = *u.Last_name
    }
    if u.Gender != nil {
        if *u.Gender == "f" {
            un.Gender = 'f'
        } else {
            un.Gender = 'm'
        }
        updateIndexAvg = true
    }
    if u.Birth_date != nil {
        un.Birth_date = *u.Birth_date
        updateIndexAvg = true
    }

    if updateIndexAvg {
        u := un

        Age := (now - u.Birth_date) / (365.24 * 24 * 3600)
        UpdateIdxLocation(User, Age, u.Gender)
    }
}

func userUpdateHandler(ctx *fasthttp.RequestCtx, User int) {
    //dumpPOST(ctx)

    var u user_update

    if unmarshal(ctx.PostBody(), &u) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    un := getUserSync(User)
    if un != nil {
        go routineUserUpdate(u, un, User)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func userInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var u user_update
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

    if getUserSync(User) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertUser(User, &u)

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Visits
*******************************************************************************/

func routineVisitUpdate(vi visit_update, vn * visit, Visit int) {
    old_location := vn.Location
    old_user := vn.User
    if vi.Location != nil {
        vn.Location = *vi.Location
    }
    if vi.User != nil {
        vn.User = *vi.User
    }
    if vi.Mark != nil {
        vn.Mark = *vi.Mark
    }
    if vi.Visited_at != nil {
        vn.Visited_at = *vi.Visited_at
    }

    v := vn
    Location := v.Location
    User := v.User
    l := getLocationSync(Location)
    u := getUserSync(User)

    // temporary item for locationsAvg
    Age := (now - u.Birth_date) / (365.24 * 24 * 3600)
    newIdxLocations := locationsAvg{v.Visited_at, Age, u.Gender, int(v.Mark)}

    // temporary item for usersVisits
    newIdxUsersVisits := usersVisits{Visit, l.Distance, l.CountryId, v.Mark, l.PlaceId}

    var idxLocationsRemoved *locationsAvg
    var idxVisitsRemoved *usersVisits

    var lr *location
    // update index /locations/:id/avg
    if old_location != Location {
        lr = getLocationSync(old_location)
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
        ur = getUserSync(old_user)
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

    u.Idx.Insert(v.Visited_at, &newIdxUsersVisits)  // add it to new_user
    iu := getIdxUser(Location)
    iu.PushBack(&newIdxUsersVisits)
}

func visitUpdateHandler(ctx *fasthttp.RequestCtx, Visit int) {
    //dumpPOST(ctx)

    var v visit_update
    if unmarshal(ctx.PostBody(), &v) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    vn := getVisitSync(Visit)
    if vn != nil {
        go routineVisitUpdate(v, vn, Visit)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func visitInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var v visit_update
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

    if getVisitSync(Visit) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertVisit(Visit, &v)
        ctx.Write([]byte("{}"))
    }
}

func locationAvgHandler(ctx *fasthttp.RequestCtx, l * location) {
    // Parse GET parameters
    qa := ctx.URI().QueryArgs()
    fromDateStr := qa.Peek("fromDate")
    toDateStr := qa.Peek("toDate")
    fromAgeStr := qa.Peek("fromAge")
    toAgeStr := qa.Peek("toAge")
    genderStr := qa.Peek("gender")

    skipGender := true
    fromDate, toDate, fromAge, toAge, gender := 0,4294967295,0,4294967295,'0'

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

    if (len(genderStr) > 0) {
        skipGender = false
        switch genderStr[0] {
        case uint8('f'):
            gender = 'f'
        case uint8('m'):
            gender = 'm'
        default:
            ctx.SetStatusCode(fasthttp.StatusBadRequest)
            return
        }
    }

    //log.Println(Location, fromDateStr, toDateStr, fromAgeStr, toAgeStr, gender);

    l.Idx.CalcAvg(ctx, skipGender, fromDate, toDate, fromAge, toAge, gender)
}

func usersVisitsHandler(ctx *fasthttp.RequestCtx, u * user) {
    // Parse GET parameters
    qa := ctx.URI().QueryArgs()
    fromDateStr := qa.Peek("fromDate")
    toDateStr := qa.Peek("toDate")
    country := string(qa.Peek("country"))
    toDistanceStr := qa.Peek("toDistance")

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

    if (len(country) > 0) {
        skipCountry = false
    }

    u.Idx.VisitsHandler(ctx, skipCountry, fromDate, toDate, countryId[country], toDistance)
}


func main () {
    log.Println("HighLoad Cup 2017 solution 41 by oioki")

    now = int(time.Now().Unix())

    // Create shared data structures
    locations = make(map[int]*location, 3969 * 2)
    users = make(map[int]*user, 4072 * 2)
    visits = make(map[int]*visit, 5951 * 2)

    IdxUser = make(map[int]*list.List, locationsMaxCount)
    IdxLocation = make(map[int]*list.List, usersMaxCount)

    country = make(map[int]string, countryMaxCount)
    countryId = make(map[string]int, countryMaxCount)
    countryCount = 0

    city = make(map[int]string, cityMaxCount)
    cityId = make(map[string]int, cityMaxCount)
    cityCount = 0

    place = make(map[int]string, placeMaxCount)
    placeId = make(map[string]int, placeMaxCount)
    placeCount = 0

    if len(os.Args) > 1 {
        loadAll("/home/oioki/dev/hlcupdocs/data/" + os.Args[1] + "/data")
    } else {
        loadAll("/root")
    }

    log.Println("You're ready, go!")

    //go warmupAll()

    fasthttp.ListenAndServe(":80", router)
}
