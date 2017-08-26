package main

import (
    "github.com/valyala/fasthttp"
//    "log"
)


func router1(ctx *fasthttp.RequestCtx) {
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
                        if (0<id && id<locationsMaxCount+1) && (locations1[id].Id > 0) {
                            locationAvgHandler(ctx, &locations1[id])
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
                        //locationSelectHandler(ctx, id)
                        // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
                        if (0<id && id<locationsMaxCount+1) && (locations1[id].Id > 0) {
                            ctx.Write(locations1[id].Raw)
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
                case 115:  // ord('g') = /users/:id/visits
                    id, err := Atoi(uri[7:lu-7])
                    if err == nil {
                        //log.Printf("%s %q %s %d", method, uri, "/users/:id/visits", id)
                        if (0<id && id<usersMaxCount+1) && (users1[id].Id > 0) {
                            usersVisitsHandler(ctx, &users1[id])
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
                        //userSelectHandler(ctx, id)
                        // Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
                        if (0<id && id<usersMaxCount+1) && (users1[id].Id > 0) {
                            ctx.Write(users1[id].Raw)
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
                    if (0<id && id<visitsMaxCount+1) && (visits1[id].Id > 0) {
                        ctx.Write(visits1[id].Raw)
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