package main

import (
	"fmt"
	"reflect"
)

func main() {
	fmt.Println("Hello, World!")

	// read from metadata
	fields := []reflect.StructField{
		{Name: "Name", Type: reflect.TypeOf("")},
		{Name: "Age", Type: reflect.TypeOf(0)},
	}
	personType := reflect.StructOf(fields)
	person := reflect.New(personType).Elem()
	person.FieldByName("Name").SetString("Alice")
	person.FieldByName("Age").SetInt(25)
	fmt.Println(person.Interface())

	// mp := make(map[int]int)

	myMapType := reflect.MapOf(reflect.TypeOf(int(0)), personType)
	myMap := reflect.MakeMap(myMapType)

	// create a new instance of the struct and set its fields
	newPerson := reflect.New(personType).Elem()
	newPerson.FieldByName("Name").SetString("Bob")
	newPerson.FieldByName("Age").SetInt(30)

	// add the struct instance to the map
	myMap.SetMapIndex(reflect.ValueOf(3), newPerson)

	// access the struct instance in the map
	fmt.Println(myMap.MapIndex(reflect.ValueOf(3)).Interface())
}
