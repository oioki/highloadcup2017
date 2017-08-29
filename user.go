package main

import (
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

const usersMaxCount = 1000070
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

func getUserInsert(User int) (*user) {
    var u * user

    if User > usersMaxCount {
        var un user
        u = &un

        users[User] = u
    } else {
        u = &users1[User]
    }

    return u
}

func getUserInsertSync(User int) (*user) {
    var u * user

    if User > usersMaxCount {
        var un user
        u = &un

        usersMutex.Lock()
        users[User] = u
        usersMutex.Unlock()
    } else {
        u = &users1[User]
    }

    return u
}

func insertUserData(u * user, uu * user_update) {
    u.Id = *uu.Id
    u.Email = *uu.Email
    u.First_name = *uu.First_name
    u.Last_name = *uu.Last_name
    if *uu.Gender == "f" {
        u.Gender = 'f'
    } else {
        u.Gender = 'm'
    }
    u.Birth_date = *uu.Birth_date
    u.Idx = NewUsersVisitsIndex()
}

func loadUser(User int, uu * user_update) {
    u := getUserInsert(User)
    insertUserData(u, uu)
}

func insertUser(User int, uu * user_update) {
    u := getUserInsertSync(User)
    insertUserData(u, uu)
}
