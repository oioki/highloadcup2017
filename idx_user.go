package main

func UpdateIdxUser(l * location, Distance int, Country string, Place []byte) {
    for k, _ := range l.Deps{
        k.Distance = Distance
        k.Country = Country
        k.Place = Place
    }
}
