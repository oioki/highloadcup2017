package main

import (
    "fmt"
    "github.com/valyala/fasthttp"
    //"log"
)


type usersVisits struct {
    Visited_at int  // sorting key, but also used in Raw rendering

    // key
    Visit int       // visit
    Distance int    // location
    CountryId int   // location

    // data
    Mark int        // visit
    PlaceId int     // location
}

type UsersVisitsNode struct {
    key      int
    val      * usersVisits
    nextNode *UsersVisitsNode
}

type UsersVisitsIndex struct {
    head     *UsersVisitsNode
}

func NewUsersVisitsIndex() UsersVisitsIndex {
    return UsersVisitsIndex{head: &UsersVisitsNode{key: 0, val: nil, nextNode: nil}}
}

func (b UsersVisitsIndex) Insert(key int, value * usersVisits) {
    //log.Println("/users:/visits", key)
    currentNode := b.head
    var previousNode *UsersVisitsNode
    newNode := &UsersVisitsNode{key: key, val: value, nextNode: nil}

    for {
        if currentNode.key > key {
            newNode.nextNode = previousNode.nextNode
            previousNode.nextNode = newNode
            return
        }

        if currentNode.nextNode == nil {
            currentNode.nextNode = newNode
            return
        }

        previousNode = currentNode
        currentNode = currentNode.nextNode
    }
}

func (b UsersVisitsIndex) RemoveByVisit(Visit int) (*usersVisits) {
    currentNode := b.head
    var previousNode *UsersVisitsNode
    for {
        if currentNode.val != nil {
            val := currentNode.val
            if val.Visit == Visit {
                previousNode.nextNode = currentNode.nextNode
                return currentNode.val
            }
        }

        if currentNode.nextNode == nil {
            return nil
        }
        previousNode = currentNode
        currentNode = currentNode.nextNode
    }
}

func (b UsersVisitsIndex) VisitsHandler(ctx *fasthttp.RequestCtx, skipCountry bool, fromDate int, toDate int, CountryId int, toDistance int) () {
    ctx.Write([]byte("{\"visits\":["))

    if b.head.nextNode == nil {  // no visits of this user
        ctx.Write([]byte("]}"))
        return
    }

    currentNode := b.head.nextNode

    first_entry := true

    for {
        val := currentNode.val
        Visited_at := currentNode.key
        //log.Println(val.Visit, Visited_at, val.Distance, val.Country, val.Mark, val.Place)

        matched :=
            (Visited_at > fromDate) &&
            (Visited_at < toDate) &&
            (skipCountry || val.CountryId == CountryId) &&
            (val.Distance < toDistance)

        if matched {
            if first_entry {
                first_entry = false
            } else {
                ctx.Write([]byte(","))
            }
            fmt.Fprintf(ctx, "{\"mark\":%d,\"visited_at\":%d,\"place\":\"%s\"}", val.Mark, Visited_at, place[val.PlaceId])
        }

        if currentNode.nextNode == nil {
            break
        }

        currentNode = currentNode.nextNode
    }
    ctx.Write([]byte("]}"))
}
