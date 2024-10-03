package indexedmap

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Person struct {
	Id        int
	FirstName string
	LastName  string
	SSN       string
}

// method define indexing key
func (r Person) SSN4() string {
	if r.SSN == "" || len(r.SSN) < 4 {
		return ""
	} else {
		return r.SSN[len(r.SSN)-4:]
	}
}

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

func TestGet(t *testing.T) {
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
		SSN:       "345343123",
	}

	persons := NewPersonMap()

	persons.PutInt(alex.Id, alex)
	persons.PutInt(john.Id, john)

	assert.Equal(t, 2, persons.Size(), "Wrong map size")
	assert.Equal(t, 2, len(persons.Keys()), "Wrong Keys size")

	n := len(persons.GetByIndex("SSN4", "3123"))
	assert.Equal(t, 2, n, "Wrong SSN4 index records number")
}

func TestImmutable(t *testing.T) {
	alex := Person{
		Id:        1,
		FirstName: "Alex",
		LastName:  "Smith",
		SSN:       "123123123",
	}

	persons := NewPersonMap()

	persons.PutInt(alex.Id, alex)

	alex.LastName = "New"

	a, _ := persons.Get(strconv.Itoa(alex.Id))
	assert.Equal(t, "Smith", a.LastName, "Returned object is not immutable")

	a.LastName = "Second"

	b, _ := persons.Get(strconv.Itoa(alex.Id))
	assert.Equal(t, "Smith", b.LastName, "Returned object is not immutable")
}

type Animal struct {
	Id      int
	Name    string
	Type    string
	Role    string
	NumType int
}

func (a Animal) RoleType() string {
	return a.Role + ":" + a.Type
}

func NewAnimalMap() *IndexedMap[Animal] {
	return NewIndexedMap(map[string]IndexFunc[Animal]{
		"Type": func(a *Animal) string {
			return a.Type
		},
		"Role": func(a *Animal) string {
			return a.Role
		},
		"NumType": func(a *Animal) string {
			return strconv.Itoa(a.NumType)
		},
		"RoleType": func(a *Animal) string {
			return a.RoleType()
		},
	})
}

func TestPut(t *testing.T) {

	m := NewAnimalMap()

	m.PutInt(1, Animal{Id: 1, Name: "Cat"})
	m.PutInt(2, Animal{Id: 2, Name: "Dog"})
	m.PutInt(3, Animal{Id: 3, Name: "Cow"})

	for i := range 3 {
		if _, ok := m.Get(strconv.Itoa(i + 1)); !ok {
			t.Fatalf("Animal %d not found", i+1)
		}
	}

	for i, n := range []string{"Cat", "Dog", "Cow"} {
		if a, ok := m.Get(strconv.Itoa(i + 1)); !ok || a.Name != n {
			t.Fatalf("Animal name %s doesn't match with received object Id: %d, Name: %s", n, a.Id, a.Name)
		}
	}
}

func TestPutSame(t *testing.T) {
	m := NewAnimalMap()

	x := Animal{Id: 1, Name: "Dog", Type: "big"}
	m.PutInt(x.Id, x)

	s := len(m.GetByIndex("Type", "big"))
	assert.Equal(t, 1, s, "Wrong map size")

	m.PutInt(x.Id, x)

	s = len(m.GetByIndex("Type", "big"))
	assert.Equal(t, 1, s, "Wrong map size")

	m.PutInt(x.Id, x)

	s = len(m.GetByIndex("Type", "big"))
	assert.Equal(t, 1, s, "Wrong map size")
}

func TestRemove(t *testing.T) {
	m := NewAnimalMap()

	sheep := Animal{Id: 6, Name: "Sheep", Type: "big"}
	cow := Animal{Id: 7, Name: "Cow", Type: "small"}

	m.PutInt(sheep.Id, sheep)
	m.PutInt(cow.Id, cow)

	a, ok := m.Remove(strconv.Itoa(cow.Id))
	assert.True(t, ok, "Error deleting Cow")
	assert.Equal(t, "Cow", a.Name, "Not a cow was deleted")

	s := len(m.GetByIndex("Type", "big"))
	assert.Equal(t, 1, s, "Wrong indexed result for big after deletion")

	s = len(m.GetByIndex("Type", "small"))
	assert.Equal(t, 0, s, "Wrong indexed result for small after deletion")
}

func TestGetByIndexTest(t *testing.T) {
	m := NewAnimalMap()

	m.PutInt(1, Animal{Id: 1, Name: "Mice", Type: "big"})
	m.PutInt(2, Animal{Id: 2, Name: "Cat", Type: "small"})
	m.PutInt(3, Animal{Id: 3, Name: "Dog", Type: "big"})
	m.PutInt(4, Animal{Id: 4, Name: "Cow", Type: "small"})
	m.PutInt(5, Animal{Id: 5, Name: "Horse", Type: "big"})
	m.PutInt(6, Animal{Id: 6, Name: "Pig", Type: "small"})
	m.PutInt(7, Animal{Id: 7, Name: "Chicken", Type: "small"})

	list := m.GetByIndex("Type", "Big")
	assert.Equal(t, 3, len(list), "Wrong indexed result")

	list = m.GetByIndex("Type", "smALL")
	assert.Equal(t, 4, len(list), "Wrong indexed result")

	types := m.GetIndexKeys("Type")
	assert.Equal(t, 2, len(types), "Wrong keys for Type")
}

func TestConcurrentPut(t *testing.T) {
	m := NewAnimalMap()

	ts := time.Now()
	count := 890000
	steps := 100
	ch := make(chan int, runtime.NumCPU())
	for b := range steps {
		go func() {
			for i := range count / steps {
				n := b*(count/steps) + i
				t := "one"
				if n%2 == 0 {
					t = "two"
				}
				m.PutInt(n, Animal{Id: n, Name: "animal" + strconv.Itoa(n), Type: t})
			}
			ch <- 1
		}()
	}
	waitChan(ch, steps)
	fmt.Printf("%d insert %v\n", count, (time.Since(ts)))

	ts = time.Now()
	for b := range steps {
		go func() {
			for i := range count / steps {
				n := b*(count/steps) + i
				t := "one"
				if n%2 == 0 {
					t = "two"
				}
				m.PutInt(n, Animal{Id: n, Name: "animal" + strconv.Itoa(n), Type: t})
			}
			ch <- 1
		}()
	}
	waitChan(ch, steps)
	fmt.Printf("%d update concurrent %v\n", count, (time.Since(ts)))

	list := []Animal{}
	for n := range count {
		t := "one"
		if n%2 == 0 {
			t = "two"
		}
		list = append(list, Animal{Id: n + count, Name: "animal" + strconv.Itoa(n+count), Type: t})
	}

	ts = time.Now()
	for b := range steps {
		go func() {
			for i := range count / steps {
				n := b*(count/steps) + i
				a := list[n]
				m.PutInt(a.Id, a)
			}
			ch <- 1
		}()
	}
	waitChan(ch, steps)
	fmt.Printf("%d additional insert concurrent %v\n", count, (time.Since(ts)))

	ts = time.Now()
	for b := range steps {
		go func() {
			for i := range count / steps {
				n := b*(count/steps) + i
				a := list[n]
				m.PutInt(a.Id, a)
			}
			ch <- 1
		}()
	}
	waitChan(ch, steps)
	fmt.Printf("%d additional update concurrent %v\n", count, (time.Since(ts)))

}

func TestConcurrentPhantomRead(t *testing.T) {
	m := NewAnimalMap()

	fmt.Println("-----------------------------------------------")

	m.PutInt(1, Animal{Id: 1, Name: "animal", Type: "one"})
	assert.False(t, len(m.GetByIndex("Type", "one")) == 0)

	count := 10000
	steps := 100
	ch := make(chan int, runtime.NumCPU())
	var wg sync.WaitGroup
	wg.Add(2)

	var ts time.Time

	go func() {
		defer wg.Done()

		ts = time.Now()
		for b := range steps {
			go func() {
				for i := range count / steps {
					n := b*(count/steps) + i + 10
					m.PutInt(n, Animal{Id: n + count, Name: "animal" + strconv.Itoa(n+count), Type: "two" + strconv.Itoa(n)})
					m.PutInt(1, Animal{Id: 1, Name: "animal", Type: "one"})
				}
				ch <- 1
			}()
		}
		waitChan(ch, steps)
		fmt.Printf("--------------------- Write %v\n", time.Since(ts))
	}()

	var reads atomic.Uint64
	rcount := 2000000
	rsteps := 100

	go func() {
		defer wg.Done()

		ts = time.Now()
		for range rsteps {
			go func() {
				for range rcount / rsteps {
					if len(m.GetByIndex("Type", "one")) == 0 {
						reads.Add(1)
					}
				}
				ch <- 1
			}()
		}
		waitChan(ch, steps)
		fmt.Printf("--------------------- Read %v\n", time.Since(ts))
	}()

	wg.Wait()

	assert.Equal(t, uint64(0), reads.Load())
}

func TestRemoveFromIndex(t *testing.T) {
	m := NewAnimalMap()

	a := Animal{Id: 1, Name: "Cat", Type: "one"}
	aa := Animal{Id: 1, Name: "Cat", Type: "one"}

	m.PutInt(1, a)
	m.PutInt(1, aa)

	assert.Equal(t, 1, len(m.GetByIndex("Type", "one")))

	a.Type = "two"

	m.PutInt(1, a)

	assert.Equal(t, 0, len(m.GetByIndex("Type", "one")))
	assert.Equal(t, 1, len(m.GetByIndex("Type", "two")))
}

func TestKeyChange(t *testing.T) {
	m := NewAnimalMap()

	rabbit := Animal{Id: 20, Name: "Rabbit", Type: "Small"}
	fox := Animal{Id: 21, Name: "Fox", Type: "BIG"}

	m.PutInt(rabbit.Id, rabbit)
	m.PutInt(fox.Id, fox)

	assert.True(t, m.ContainsKeyInt(20))
	assert.Equal(t, 1, len(m.GetByIndex("Type", "small")))
	assert.Equal(t, 1, len(m.GetByIndex("Type", "big")))

	rabbit.Type = "Big"

	m.PutInt(rabbit.Id, rabbit)

	assert.True(t, m.ContainsKeyInt(20))
	assert.Equal(t, 0, len(m.GetByIndex("Type", "small")))
	assert.Equal(t, 2, len(m.GetByIndex("Type", "big")))
}

func TestPutAll(t *testing.T) {
	m := NewAnimalMap()

	ts := time.Now()
	var data []Animal
	for i := range 1000000 {
		t := "BIG"
		if i%2 == 0 {
			t = "small"
		}
		data = append(data, Animal{Id: i, Name: "animal-" + strconv.Itoa(i), Type: t})
	}
	fmt.Printf("Data generated %v\n", time.Since(ts))

	ts = time.Now()
	m.PutAll(data, func(a *Animal) string { return strconv.Itoa(a.Id) })
	assert.Equal(t, len(data), len(m.Keys()))
	fmt.Printf("Data added to index %v\n", time.Since(ts))
}

func TestKeyCase(t *testing.T) {
	m := NewAnimalMap()

	x := Animal{Id: 1, Name: "Dog", Type: "big"}

	m.Put("test", x)

	_, ok := m.Get("Test")

	assert.True(t, ok)

}
