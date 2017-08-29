package main

func UpdateIdxUser(l * location, Distance int, CountryId int, PlaceId int) {
    for k, _ := range l.Deps{
        k.Distance = Distance
        k.CountryId = CountryId
        k.PlaceId = PlaceId
    }
}
