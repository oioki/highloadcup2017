package main

import (
    "github.com/valyala/fasthttp"
    "log"
)

var _ = log.Printf

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
                        l := getLocation(id)
                        if l != nil {
                            locationAvgHandler(ctx, l)
                        } else {
                            ctx.SetStatusCode(fasthttp.StatusNotFound)
                        }
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                default:
                    id, err := Atoi(uri[11:lu])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/locations/:id", id)
                        l := getLocation(id)
                        if l != nil {
                            ctx.Write([]byte("{\"id\":"))
                            WriteInt(ctx, id)
                            ctx.Write([]byte(",\"place\":\""))
                            ctx.Write([]byte(place[l.PlaceId]))
                            ctx.Write([]byte("\",\"country\":\""))
                            ctx.Write([]byte(country[l.CountryId]))
                            ctx.Write([]byte("\",\"city\":\""))
                            ctx.Write([]byte(city[l.CityId]))
                            ctx.Write([]byte("\",\"distance\":"))
                            WriteInt(ctx, l.Distance)
                            ctx.Write([]byte("}"))
                        } else {
                            ctx.SetStatusCode(fasthttp.StatusNotFound)
                        }
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                }

            case 117:  // = ord('u') = /users
                last_char := uri[lu-1]
                switch last_char {
                case 115:  // ord('s') = /users/:id/visits
                    id, err := Atoi(uri[7:lu-7])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id/visits", id)
                        u := getUser(id)
                        if u != nil {
                            usersVisitsHandler(ctx, u)
                        } else {
                            ctx.SetStatusCode(fasthttp.StatusNotFound)
                        }
                    } else {
                        ctx.SetStatusCode(fasthttp.StatusBadRequest)
                    }
                default:  // GET /users/:id
                    id, err := Atoi(uri[7:lu])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id", id)
                        u := getUser(id)
                        if u != nil {
                            ctx.Write([]byte("{\"id\":"))
                            WriteInt(ctx, id)
                            ctx.Write([]byte(",\"email\":\""))
                            ctx.Write(u.Email)
                            ctx.Write([]byte("\",\"first_name\":\""))
                            ctx.Write(u.First_name)
                            ctx.Write([]byte("\",\"last_name\":\""))
                            ctx.Write(u.Last_name)
                            ctx.Write([]byte("\",\"gender\":\""))
                            ctx.Write([]byte{uint8(u.Gender)})
                            ctx.Write([]byte("\",\"birth_date\":"))
                            WriteInt(ctx, u.Birth_date)
                            ctx.Write([]byte("}"))
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
                    v := getVisit(id)
                    if v != nil {
                        ctx.Write([]byte("{\"id\":"))
                        WriteInt(ctx, id)
                        ctx.Write([]byte(",\"location\":"))
                        WriteInt(ctx, v.Location)
                        ctx.Write([]byte(",\"user\":"))
                        WriteInt(ctx, v.User)
                        ctx.Write([]byte(",\"mark\":"))
                        ctx.Write([]byte{uint8(v.Mark) + '0'})
                        ctx.Write([]byte(",\"visited_at\":"))
                        WriteInt(ctx, v.Visited_at)
                        ctx.Write([]byte("}"))
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
