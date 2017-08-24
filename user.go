package main

import (
    "fmt"
    "sync"
)

type user struct {
    Id          * int
    Email       * string
    First_name  * string
    Last_name   * string
    Gender      * string
    Birth_date  * int

    Raw           []byte

    Idx         UsersVisitsIndex
}

var users map[int]*user
var usersMutex sync.RWMutex

func getUser(User int) (*user, bool) {
    usersMutex.RLock()
    l, err := users[User]
    usersMutex.RUnlock()
    return l, err
}

func insertRawUser(User int, u * user) {
    usersMutex.Lock()
    users[User] = u
    usersMutex.Unlock()
    u.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, *u.Email, *u.First_name, *u.Last_name, *u.Gender, *u.Birth_date))
}

func updateRawUser(User int, u * user) {
    u.Raw = []byte(fmt.Sprintf("{\"id\":%d,\"email\":\"%s\",\"first_name\":\"%s\",\"last_name\":\"%s\",\"gender\":\"%s\",\"birth_date\":%d}", User, *u.Email, *u.First_name, *u.Last_name, *u.Gender, *u.Birth_date))
}
