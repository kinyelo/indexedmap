# IndexedMap 

Threadsafe concurrent hashmap with primary and secondary indexes.
Under the hood uses https://github.com/puzpuzpuz/xsync Map.

*Limitations:*

- All primary and seconday index keys are strings
- All index keys are case insensitive
- Secondary indexes are updated after primary that leads to eventual consistency
- On insert/delete, record can be seen in the primary index but not found in the secondary indexes

*Usage example:*

```go
type Person struct {
	Id        int
	FirstName string
	LastName  string
	SSN       string
}

// method defines indexing key
func (r Person) SSN4() string {
	if r.SSN == "" || len(r.SSN) < 4 {
		return ""
	} else {
		return r.SSN[len(r.SSN)-4:]
	}
}

// Each index is defined as a function
func NewPersonMap() *IndexedMap[Person] {
	return NewIndexedMap(map[string]IndexFunc[Person]{
		"SSN": func(r *Person) string {
			return r.SSN
		},
		"SSN4": func(r *Person) string {
			return r.SSN4()
		},
		"LastName": func(r *Person) string {
			return r.LastName
		},
	})
}

func Test() {
	alex := Person{
		Id:        1,
		FirstName: "Alex",
		LastName:  "Smith",
		SSN:       "123123123",
	}

	john := Person{
		Id:        2,
		FirstName: "John",
		LastName:  "Doe",
		SSN:       "456453123",
	}

	persons := NewPersonMap()

    // Put by primary index
	persons.Put("2", john)
	// helper method, converts key to string inside
    persons.PutInt(alex.Id, alex)

    // Get record by primary index
    a, ok := persons.Get("1")
    // helper method, converts key to string inside
    b, ok := persons.GetInt(2)

    // Get records by secondary index
	list1 := persons.GetByIndex("SSN4", "3123")
	list2 := persons.GetByIndex("LastName", "doe")

}
```

# License

Licensed under MIT.
