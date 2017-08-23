package main

import (
    "github.com/valyala/fasthttp"
    //"log"
)


type usersVisits struct {
    Visited_at int  // sorting key, but also used in Raw rendering

    // key
    Visit int       // visit
    Distance int    // location
    Country string  // location

    // data
    Mark int        // visit
    Place string    // location

    Raw  []byte
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
    var empty * usersVisits
    return UsersVisitsIndex{head: &UsersVisitsNode{key: 0, val: empty, nextNode: nil}}
}

func (b UsersVisitsIndex) Insert(key int, value * usersVisits) {
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

func (b UsersVisitsIndex) VisitsHandler(ctx *fasthttp.RequestCtx, skipFromDate bool, skipToDate bool, skipCountry bool, skipToDistance bool, fromDate int, toDate int, country string, toDistance int) () {
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
            (skipFromDate || Visited_at > fromDate) &&
            (skipToDate || Visited_at < toDate) &&
            (skipCountry || val.Country == country) &&
            (skipToDistance || val.Distance < toDistance)

        if matched {
            if first_entry {
                first_entry = false
            } else {
                ctx.Write([]byte(","))
            }
            ctx.Write(val.Raw)
        }

        if currentNode.nextNode == nil {
            break
        }

        currentNode = currentNode.nextNode
    }
    ctx.Write([]byte("]}"))
}
