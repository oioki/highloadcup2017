package main

import (
    "bufio"
    "github.com/valyala/fasthttp"
    "log"
    "os"
    "runtime/debug"
    "strconv"
)

var now int

/*******************************************************************************
* Locations
*******************************************************************************/

func routineLocationUpdate(lu location_update, l * location, Location int) {
    updateIndexVisits := false
    if lu.Place != nil {
        l.Place = []byte(*lu.Place)
        updateIndexVisits = true
    }
    if lu.Country != nil {
        l.Country = []byte(*lu.Country)
        updateIndexVisits = true
    }
    if lu.City != nil {
        l.City = []byte(*lu.City)
    }
    if lu.Distance != nil {
        l.Distance = *lu.Distance
        updateIndexVisits = true
    }

    if updateIndexVisits {
        // update all IdxUsers which depends on this Location
        for k, _ := range l.Deps{
            k.Distance = l.Distance
            k.Country = string(l.Country)
            k.Place = l.Place
        }
    }
}

func locationUpdateHandler(ctx *fasthttp.RequestCtx, Location int) {
    //dumpPOST(ctx)

    var lu location_update
    if unmarshal(ctx.PostBody(), &lu) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    // update fields
    l := getLocation(Location)
    if l != nil {
        go routineLocationUpdate(lu, l, Location)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func locationInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var lu location_update
    if unmarshal(ctx.PostBody(), &lu) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    incomplete_data :=
        lu.Id == nil ||
        lu.Place == nil ||
        lu.Country == nil ||
        lu.City == nil ||
        lu.Distance == nil
    if incomplete_data {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    Location := *(lu.Id)

    if getLocation(Location) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertLocation(Location, &lu)

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Users
*******************************************************************************/

func routineUserUpdate(uu user_update, u * user, User int) {
    updateIndexAvg := false
    if uu.Email != nil {
        u.Email = []byte(*uu.Email)
    }
    if uu.First_name != nil {
        u.First_name = []byte(*uu.First_name)
    }
    if uu.Last_name != nil {
        u.Last_name = []byte(*uu.Last_name)
    }
    if uu.Gender != nil {
        if *uu.Gender == "f" {
            u.Gender = 'f'
        } else {
            u.Gender = 'm'
        }
        updateIndexAvg = true
    }
    if uu.Birth_date != nil {
        u.Birth_date = *uu.Birth_date
        updateIndexAvg = true
    }

    if updateIndexAvg {
        Age := (now - u.Birth_date) / (365.25 * 24 * 3600)
        for k, _ := range u.Deps{
            k.Age = Age
            k.Gender = u.Gender
        }
    }
}

func userUpdateHandler(ctx *fasthttp.RequestCtx, User int) {
    //dumpPOST(ctx)

    var uu user_update

    if unmarshal(ctx.PostBody(), &uu) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    u := getUser(User)
    if u != nil {
        go routineUserUpdate(uu, u, User)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func userInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var uu user_update
    if unmarshal(ctx.PostBody(), &uu) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    incomplete_data :=
        uu.Id == nil ||
        uu.Email == nil ||
        uu.First_name == nil ||
        uu.Last_name == nil ||
        uu.Gender == nil ||
        uu.Birth_date == nil
    if incomplete_data {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    User := *(uu.Id)

    if getUser(User) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertUser(User, &uu)

        ctx.Write([]byte("{}"))
    }
}



/*******************************************************************************
* Visits
*******************************************************************************/

func routineVisitUpdate(vu visit_update, v * visit, Visit int) {
    old_location := v.Location
    old_user := v.User
    if vu.Location != nil {
        v.Location = *vu.Location
    }
    if vu.User != nil {
        v.User = *vu.User
    }
    if vu.Mark != nil {
        v.Mark = *vu.Mark
    }
    if vu.Visited_at != nil {
        v.Visited_at = *vu.Visited_at
    }

    Location := v.Location
    User := v.User
    l := getLocation(Location)
    u := getUser(User)

    // temporary item for locationsAvg
    Age := (now - u.Birth_date) / (365.25 * 24 * 3600)
    newIdxLocations := locationsAvg{v.Visited_at, Age, u.Gender, int(v.Mark)}

    // temporary item for usersVisits
    newIdxUsersVisits := usersVisits{Visit, l.Distance, string(l.Country), v.Mark, l.Place}

    var idxLocationsRemoved *locationsAvg
    var idxVisitsRemoved *usersVisits

    var lr *location
    // update index /locations/:id/avg
    if old_location != Location {
        lr = getLocation(old_location)
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
        ur = getUser(old_user)
    } else {
        ur = u
    }

    if ur != nil {  // if old user existed
        //log.Printf("deleting item (%d) from users[remove_user=%d] index (UsersVisits)", User, remove_user)
        idxVisitsRemoved = ur.Idx.RemoveByVisit(Visit)
    }

    // remove this index from dependency list of IdxUser[old_location]
    if old_location != Location {
        delete(lr.Deps, idxVisitsRemoved)
    }

    // remove this index from dependency list of IdxLocation[old_user]
    if old_user != User {
        delete(ur.Deps, idxLocationsRemoved)
    }

    l.Idx.Insert(Visit, &newIdxLocations)  // add it to new_location
    getUser(User).Deps[&newIdxLocations] = true

    u.Idx.Insert(v.Visited_at, &newIdxUsersVisits)  // add it to new_user
    getLocation(Location).Deps[&newIdxUsersVisits] = true
}

func visitUpdateHandler(ctx *fasthttp.RequestCtx, Visit int) {
    //dumpPOST(ctx)

    var vu visit_update
    if unmarshal(ctx.PostBody(), &vu) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    v := getVisit(Visit)
    if v != nil {
        go routineVisitUpdate(vu, v, Visit)
        ctx.Write([]byte("{}"))
    } else {
        ctx.SetStatusCode(fasthttp.StatusNotFound)
    }
}

func visitInsertHandler(ctx *fasthttp.RequestCtx) {
    //dumpPOST(ctx)

    var vu visit_update
    if unmarshal(ctx.PostBody(), &vu) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    incomplete_data :=
        vu.Id == nil ||
        vu.Location == nil ||
        vu.User == nil ||
        vu.Mark == nil ||
        vu.Visited_at == nil
    if incomplete_data {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        return
    }

    Visit := *(vu.Id)

    if getVisit(Visit) != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
    } else {
        go insertVisit(Visit, &vu)
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

    u.Idx.VisitsHandler(ctx, skipCountry, fromDate, toDate, country, toDistance)
}


func main () {
    log.Println("HighLoad Cup 2017 solution 47 by oioki")

    // disable garbage collection
    debug.SetGCPercent(-1)

    // working directory
    dir := "/root"
    options_dir := "/tmp/data"
    if len(os.Args) > 1 {
        dir = "/home/oioki/dev/hlcupdocs/data/" + os.Args[1] + "/data"
        options_dir = dir
    }

    file, err := os.Open(options_dir + "/options.txt")
    if err != nil {
        log.Fatal(err)
    }
    scanner := bufio.NewScanner(file)
    scanner.Scan()
    now, err = strconv.Atoi(scanner.Text())
    log.Println("options.now =", now)
    if err != nil {
        log.Fatal(err)
    }
    file.Close()

    loadAll(dir)

    log.Println("You're ready, go!")

    fasthttp.ListenAndServe(":80", router)
}
