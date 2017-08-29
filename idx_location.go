package main

func UpdateIdxLocation(u * user, Age int, Gender rune) {
    for k, _ := range u.Deps{
        k.Age = Age
        k.Gender = Gender
    }
}
