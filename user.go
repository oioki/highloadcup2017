package main

import (
//    "fmt"
    "log"
    "sync"
)

var _ = log.Println


type user_update struct {
    Id          * int
    Email       * string
    First_name  * string
    Last_name   * string
    Gender      * string
    Birth_date  * int
}

type user struct {
    Id          int
    Email       string
    First_name  string
    Last_name   string
    Gender      rune
    Birth_date  int

    Idx         UsersVisitsIndex
}

var users map[int]*user
var usersMutex sync.RWMutex

const usersMaxCount = 1000074
var usersCount int
var users1[usersMaxCount+1]user

// Note: as there are no write requests (POST) on phases 1 and 3, we may skip mutex locking
func getUser(User int) (*user) {
    if User <= usersMaxCount {
        if users1[User].Id == 0 {
            return nil
        }
        return &users1[User]
    }

    return users[User]
}

func getUserSync(User int) (*user) {
    if User <= usersMaxCount {
        if users1[User].Id == 0 {
            return nil
        }
        return &users1[User]
    }

    usersMutex.RLock()
    u := users[User]
    usersMutex.RUnlock()
    return u
}

func loadUser(User int, u * user_update) {
    un := &users1[User]
    un.Id = User
    un.Email = *u.Email
    un.First_name = *u.First_name
    un.Last_name = *u.Last_name
    if *u.Gender == "f" {
        un.Gender = 'f'
    } else {
        un.Gender = 'm'
    }
    un.Birth_date = *u.Birth_date
    un.Idx = NewUsersVisitsIndex()
}

func insertUser(User int, u * user_update) {
    var ul * user

    if User > usersMaxCount {
        var un user
        ul = &un

        usersMutex.Lock()
        users[User] = ul
        usersMutex.Unlock()
    } else {
        ul = &users1[User]
    }

    ul.Id = User
    ul.Email = *u.Email
    ul.First_name = *u.First_name
    ul.Last_name = *u.Last_name
    if *u.Gender == "f" {
        ul.Gender = 'f'
    } else {
        ul.Gender = 'm'
    }
    ul.Birth_date = *u.Birth_date
    ul.Idx = NewUsersVisitsIndex()
}
