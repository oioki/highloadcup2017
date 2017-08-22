package main

import (
    "fmt"
    //"log"
)


type locationsAvg struct {
    // sort key = Visit

    // key
    Visited_at int  // visit
    Age        int  // user
    Gender string   // user

    // data
    Mark int        // visit
}




type LocationsAvgNode struct {
    key      int
    val      *locationsAvg
    nextNode *LocationsAvgNode
}

type LocationsAvgIndex struct {
    head     *LocationsAvgNode
}

func NewLocationsAvgIndex() *LocationsAvgIndex {
    var empty * locationsAvg
    return &LocationsAvgIndex{head: &LocationsAvgNode{key: 0, val: empty, nextNode: nil}}
}

func (b *LocationsAvgIndex) Insert(key int, value * locationsAvg) {
    if b.head == nil {
        // node is empty
        b.head = &LocationsAvgNode{key: key, val: value, nextNode: nil}
    } else {
        currentNode := b.head
        var previousNode *LocationsAvgNode
        newNode := &LocationsAvgNode{key: key, val: value, nextNode: nil}

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
}

func (b *LocationsAvgIndex) Remove(key int) (*locationsAvg) {
    currentNode := b.head
    var previousNode *LocationsAvgNode
    for {
        if currentNode.key == key {
            previousNode.nextNode = currentNode.nextNode
            return currentNode.val
        }

        if currentNode.nextNode == nil {
            return nil
        }
        previousNode = currentNode
        currentNode = currentNode.nextNode
    }
    return nil
}

func (b *LocationsAvgIndex) CalcAvg(skipFromDate bool, skipToDate bool, skipFromAge bool, skipToAge bool, skipGender bool, fromDate int, toDate int, fromAge int, toAge int, gender string)(avg string) {
    if b.head.nextNode == nil {  // no marks of this location
        return "0.0"
    }

    currentNode := b.head.nextNode
    sum := 0
    cnt := 0
    for {
        val := currentNode.val

        matched :=
            (skipFromDate || val.Visited_at > fromDate) &&
            (skipToDate || val.Visited_at < toDate) &&
            (skipFromAge || val.Age >= fromAge) &&
            (skipToAge || val.Age < toAge) &&
            (skipGender || gender == val.Gender)

        if matched {
            //log.Println("matched", val.Visited_at, val.Birth_date, val.Gender, val.Mark)
            sum += val.Mark
            cnt++
        }

        if currentNode.nextNode == nil {
            break
        }
        currentNode = currentNode.nextNode
    }

    if cnt == 0 {
        return "0.0"
    }
    return fmt.Sprintf("%.6g", float64(sum) / float64(cnt))
}
