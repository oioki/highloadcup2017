package main

import (
//    "fmt"
    _"log"
    "sync"
)

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

    //Raw           []byte

    Idx         UsersVisitsIndex
}

type user1 struct {
    Id          int
    Email       string
    First_name  string
    Last_name   string
    Gender      rune
    Birth_date  int

//    Raw         []byte

    Idx         UsersVisitsIndex
}

var users map[int]*user
var usersMutex sync.RWMutex

const usersMaxCount = 1000074 + 40000
var usersCount int
//var users1[usersMaxCount+1]user1
var users1[1]user1

func getUser(User int) (*user, bool) {
    usersMutex.RLock()
    l, err := users[User]
    usersMutex.RUnlock()
    return l, err
}

func insertRawUserLoad(User int, u * user_update) {
    // if 'u' were of type 'user'
    //users[User] = u
    //u.Idx = NewUsersVisitsIndex()

    var un user
    users[User] = &un
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

func insertRawUser(User int, u * user_update) {
    usersMutex.Lock()
    var un user
    users[User] = &un
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
    usersMutex.Unlock()
    //u.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, *u.Email, *u.First_name, *u.Last_name, *u.Gender, *u.Birth_date))
}

func updateRawUser(User int, u * user) {
    //u.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, *u.Email, *u.First_name, *u.Last_name, *u.Gender, *u.Birth_date))
}

func updateRawUser1(User int) {
    //users1[User].Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, users1[User].Email, users1[User].First_name, users1[User].Last_name, users1[User].Gender, users1[User].Birth_date))
}
