package main

import (
    "log"
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
    Email       []byte
    First_name  []byte
    Last_name   []byte
    Gender      rune
    Birth_date  int

    Idx         UsersVisitsIndex
    Deps        map[*locationsAvg]bool
}

const usersMaxCount = 1000070 + 10000
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

    return nil;
}

func getUserInsert(User int) (*user) {
    return &users1[User]
}

func insertUserData(u * user, uu * user_update) {
    u.Id = *uu.Id
    u.Email = []byte(*uu.Email)
    u.First_name = []byte(*uu.First_name)
    u.Last_name = []byte(*uu.Last_name)
    if *uu.Gender == "f" {
        u.Gender = 'f'
    } else {
        u.Gender = 'm'
    }
    u.Birth_date = *uu.Birth_date
    u.Idx = NewUsersVisitsIndex()
    u.Deps = make(map[*locationsAvg]bool, 20)
}

func loadUser(User int, uu * user_update) {
    u := getUserInsert(User)
    insertUserData(u, uu)
}

func insertUser(User int, uu * user_update) {
    u := getUserInsert(User)
    insertUserData(u, uu)
}
